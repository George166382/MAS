"""
api_gateway.py
--------------
Two responsibilities only:
  POST /upload          - accepts audio, publishes to Kafka, returns trace_id
  GET  /result/{trace_id} - polls Result DB, returns status/result
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
from kafka import KafkaConsumer

import result_store
from audio_producer import send_audio
from result_store import Status

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("api_gateway")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_RESULT_TOPIC = os.getenv("KAFKA_RESULT_TOPIC", "analysis.misinformation.raw")
DLQ_TOPICS = [
    "audio.raw.dlq",
    "audio.transcribed.dlq",
    "analysis.claims.dlq",
]


async def kafka_result_consumer():
    loop = asyncio.get_event_loop()
    consumer = await loop.run_in_executor(None, lambda: KafkaConsumer(
        KAFKA_RESULT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="api-gateway-result-consumer",
    ))

    log.info("Result consumer listening on '%s'", KAFKA_RESULT_TOPIC)

    while True:
        try:
            raw = await loop.run_in_executor(None, lambda: consumer.poll(timeout_ms=500))
            for _, messages in raw.items():
                for msg in messages:
                    trace_id = msg.value.get("metadata", {}).get("trace_id")
                    payload = msg.value.get("payload")
                    if not trace_id:
                        log.warning("Message without trace_id - skipping")
                        continue
                    result_store.update(trace_id, Status.COMPLETED, result=payload)
                    log.info("Result stored for trace_id=%s", trace_id)
        except Exception as exc:
            log.error("Kafka consumer error: %s", exc)
            await asyncio.sleep(1)


async def kafka_dlq_consumer():
    loop = asyncio.get_event_loop()
    consumer = await loop.run_in_executor(None, lambda: KafkaConsumer(
        *DLQ_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="api-gateway-dlq-consumer",
    ))

    log.info("DLQ consumer listening on %s", DLQ_TOPICS)

    while True:
        try:
            raw = await loop.run_in_executor(None, lambda: consumer.poll(timeout_ms=500))
            for tp, messages in raw.items():
                for msg in messages:
                    trace_id = msg.value.get("metadata", {}).get("trace_id")
                    error = msg.value.get("error", "Pipeline failure")
                    if not trace_id:
                        log.warning("DLQ message without trace_id on topic %s", tp.topic)
                        continue
                    result_store.update(trace_id, Status.FAILED, error=error)
                    log.warning("Marked trace_id=%s as FAILED (topic=%s): %s", trace_id, tp.topic, error)
        except Exception as exc:
            log.error("DLQ consumer error: %s", exc)
            await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    result_task = asyncio.create_task(kafka_result_consumer())
    dlq_task = asyncio.create_task(kafka_dlq_consumer())
    log.info("Background Kafka consumers started")
    yield
    for task in (result_task, dlq_task):
        task.cancel()
    for task in (result_task, dlq_task):
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)

AUDIO_DIR = "audio"
os.makedirs(AUDIO_DIR, exist_ok=True)


@app.post("/upload")
async def upload_audio(file: UploadFile = File(...)):
    save_path = os.path.join(AUDIO_DIR, file.filename)

    try:
        with open(save_path, "wb") as buffer:
            while chunk := await file.read(256 * 1024):
                buffer.write(chunk)
            buffer.flush()
            os.fsync(buffer.fileno())
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to save uploaded file") from exc
    finally:
        await file.close()

    file_size = os.path.getsize(save_path)
    log.info("Saved '%s' (%d bytes)", file.filename, file_size)

    try:
        trace_id = send_audio(save_path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to enqueue audio") from exc

    result_store.create(trace_id)

    return {
        "status": "accepted",
        "trace_id": trace_id,
        "poll_url": f"/result/{trace_id}",
        "size_bytes": file_size,
    }


@app.get("/result/{trace_id}")
async def get_result(trace_id: str):
    row = result_store.get(trace_id)
    if row is None:
        raise HTTPException(status_code=404, detail="trace_id not found")
    return JSONResponse(content=row)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5000)