import logging
import os

from fastapi import FastAPI, HTTPException, UploadFile, File

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
# App
# ---------------------------------------------------------------------------
app = FastAPI()

AUDIO_DIR = "audio"
os.makedirs(AUDIO_DIR, exist_ok=True)


@app.post("/upload")
async def upload_audio(file: UploadFile = File(...)):
    save_path = os.path.join(AUDIO_DIR, file.filename)

    # ------------------------------------------------------------------ #
    # Step 1 — Write the file exactly once and fsync before closing.      #
    # Opening in "wb" truncates any existing file, so we must never       #
    # open the same path a second time before the write is complete.      #
    # ------------------------------------------------------------------ #
    try:
        with open(save_path, "wb") as buffer:
            while chunk := await file.read(1024 * 256):  # 256 KB chunks
                buffer.write(chunk)
            buffer.flush()
            os.fsync(buffer.fileno())  # guarantee bytes hit disk before we tell Kafka
    except Exception as exc:
        log.error("Failed to save uploaded file '%s': %s", file.filename, exc)
        raise HTTPException(status_code=500, detail="Failed to save uploaded file.") from exc
    finally:
        await file.close()

    file_size = os.path.getsize(save_path)
    log.info("Saved '%s' (%d bytes) to %s", file.filename, file_size, save_path)

    # ------------------------------------------------------------------ #
    # Step 2 — = publish to Kafka. 
    # ------------------------------------------------------------------ #
    try:
        send_audio(save_path)
    except Exception as exc:
        log.error("File saved but failed to trigger audio pipeline: %s", exc)
        raise HTTPException(
            status_code=500,
            detail="File saved but failed to trigger audio pipeline.",
        ) from exc

    return {"status": "success", "file_path": save_path, "size_bytes": file_size}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)