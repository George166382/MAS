import asyncio
import json
import logging
import os

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, UploadFile, File, WebSocket, WebSocketDisconnect
from kafka import KafkaConsumer
from audio_producer import send_audio

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("upload_server")

# ---------------------------------------------------------------------------
# A. Connection Manager — tracks which WebSocket is waiting for which trace_id
# ---------------------------------------------------------------------------
class ConnectionManager:
    def __init__(self):
        # Maps trace_id → WebSocket so the Kafka consumer can route results
        self.active_connections: dict[str, WebSocket] = {}

    async def connect(self, trace_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[trace_id] = websocket
        log.info("WebSocket connected for trace_id=%s (total active: %d)",
                 trace_id, len(self.active_connections))

    async def send_result(self, trace_id: str, result: dict):
        """Push result to the waiting client and clean up."""
        websocket = self.active_connections.get(trace_id)
        if websocket is None:
            log.warning("No active WebSocket for trace_id=%s — result dropped", trace_id)
            return
        try:
            await websocket.send_json(result)
            log.info("Sent result to trace_id=%s", trace_id)
        except Exception as e:
            log.error("Failed to send result to trace_id=%s: %s", trace_id, e)
        finally:
            await self.disconnect(trace_id)

    async def disconnect(self, trace_id: str):
        websocket = self.active_connections.pop(trace_id, None)
        if websocket:
            try:
                await websocket.close()
            except Exception:
                pass  # already closed by client
            log.info("WebSocket closed for trace_id=%s (remaining: %d)",
                     trace_id, len(self.active_connections))

manager = ConnectionManager()

# ---------------------------------------------------------------------------
# B. Background Kafka Consumer — listens on analysis.misinformation.final
# ---------------------------------------------------------------------------
KAFKA_RESULT_TOPIC = "analysis.misinformation.raw"

async def kafka_result_consumer():
    """
    Runs as a background asyncio task for the lifetime of the server.
    Polls Kafka synchronously in a thread-safe way using run_in_executor
    so it never blocks the asyncio event loop.
    """
    loop = asyncio.get_event_loop()

    # KafkaConsumer is synchronous — we run its poll inside an executor
    # so FastAPI's async event loop is never blocked.
    consumer = await loop.run_in_executor(None, lambda: KafkaConsumer(
        KAFKA_RESULT_TOPIC,
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="api-gateway-result-consumer",
    ))

    log.info("Kafka result consumer listening on topic '%s'", KAFKA_RESULT_TOPIC)

    while True:
        try:
            # poll() with a short timeout so we yield back to asyncio regularly
            raw_messages = await loop.run_in_executor(
                None, lambda: consumer.poll(timeout_ms=500)
            )

            for _, messages in raw_messages.items():
                for msg in messages:
                    payload = msg.value  # already deserialized to dict

                    # C. Routing Logic
                    trace_id = payload.get("metadata", {}).get("trace_id")
                    result   = payload.get("payload")

                    if not trace_id:
                        log.warning("Received message without trace_id — skipping")
                        continue

                    log.info("Routing result for trace_id=%s", trace_id)

                    # Push to the waiting WebSocket (closes it when done)
                    await manager.send_result(trace_id, {"trace_id": trace_id, "result": result})

        except Exception as e:
            log.error("Kafka consumer error: %s", e)
            await asyncio.sleep(1)   # brief back-off before retrying

# ---------------------------------------------------------------------------
# Lifespan — starts the background consumer when the server boots
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the Kafka result consumer as a background task on startup
    task = asyncio.create_task(kafka_result_consumer())
    log.info("Background Kafka result consumer started")
    yield
    # Cancel the consumer gracefully on shutdown
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        log.info("Kafka result consumer stopped")

app = FastAPI(lifespan=lifespan)

AUDIO_DIR = "audio"
os.makedirs(AUDIO_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Existing upload endpoint — unchanged
# ---------------------------------------------------------------------------
@app.post("/upload")
async def upload_audio(file: UploadFile = File(...)):
    save_path = os.path.join(AUDIO_DIR, file.filename)

    try:
        with open(save_path, "wb") as buffer:
            while chunk := await file.read(1024 * 256):
                buffer.write(chunk)
            buffer.flush()
            os.fsync(buffer.fileno())
    except Exception as exc:
        log.error("Failed to save uploaded file '%s': %s", file.filename, exc)
        raise HTTPException(status_code=500, detail="Failed to save uploaded file.") from exc
    finally:
        await file.close()

    file_size = os.path.getsize(save_path)
    log.info("Saved '%s' (%d bytes) to %s", file.filename, file_size, save_path)

    try:
        trace_id = send_audio(save_path)
    except Exception as exc:
        log.error("File saved but failed to trigger audio pipeline: %s", exc)
        raise HTTPException(
            status_code=500,
            detail="File saved but failed to trigger audio pipeline.",
        ) from exc

    return {"status": "success", "file_path": save_path, "size_bytes": file_size, "trace_id": trace_id}

# ---------------------------------------------------------------------------
# A. WebSocket endpoint — mobile client connects here to wait for its result
# ---------------------------------------------------------------------------
@app.websocket("/ws/{trace_id}")
async def websocket_endpoint(websocket: WebSocket, trace_id: str):
    """
    The Android app connects here immediately after uploading audio.
    It passes the same trace_id that was stamped on the Kafka message
    by the audio pipeline, so the result consumer can route back to it.

    The connection stays open until:
      - The analysis result arrives (server sends JSON then closes), or
      - The client disconnects early (e.g. app backgrounded)
    """
    await manager.connect(trace_id, websocket)
    try:
        # Keep the socket alive — we only send once (from the Kafka consumer)
        # but we need to handle unexpected disconnects from the client side.
        while True:
            await websocket.receive_text()   # blocks; raises on disconnect
    except WebSocketDisconnect:
        log.info("Client disconnected early for trace_id=%s", trace_id)
        await manager.disconnect(trace_id)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
