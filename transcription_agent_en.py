"""
Transcription Agent
-------------------
Consumes raw audio messages from Kafka, validates and converts audio files
to WAV via ffmpeg, transcribes with Whisper, and publishes results.

Handles the common M4A "moov atom not found" failure by:
  1. Validating the file has a decodable audio stream (ffprobe)
  2. Transcoding to 16kHz mono WAV before passing to Whisper
  3. Retrying both validation and transcription with configurable back-off
"""

import json
import logging
import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid

import whisper
from kafka import KafkaConsumer, KafkaProducer

from utils import create_metadata

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("transcription_agent")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP",   "localhost:9092")
INPUT_TOPIC       = os.getenv("INPUT_TOPIC",        "audio.raw")
OUTPUT_TOPIC      = os.getenv("OUTPUT_TOPIC",       "audio.transcribed")
DLQ_TOPIC         = os.getenv("DLQ_TOPIC",          "audio.raw.dlq")
WHISPER_MODEL     = os.getenv("WHISPER_MODEL",      "base")
LANGUAGE          = os.getenv("TRANSCRIPTION_LANG", "en")  # ISO 639-1 code, e.g. 'en' for English

FILE_READY_TIMEOUT  = int(os.getenv("FILE_READY_TIMEOUT",  "15"))   # seconds
FILE_POLL_INTERVAL  = float(os.getenv("FILE_POLL_INTERVAL", "0.5"))  # seconds

MAX_RETRIES   = int(os.getenv("MAX_RETRIES",   "5"))
RETRY_DELAY   = float(os.getenv("RETRY_DELAY", "1.0"))  # seconds, doubled each attempt

KAFKA_POLL_MS = int(os.getenv("KAFKA_POLL_MS", "1000"))

# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------
log.info("Loading Whisper model '%s'…", WHISPER_MODEL)
model = whisper.load_model(WHISPER_MODEL)
log.info("Whisper model loaded.")

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=f"transcription-agent-{uuid.uuid4()}",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
def _shutdown(sig, frame):
    log.info("Received signal %s — shutting down gracefully…", sig)
    consumer.close()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

# ---------------------------------------------------------------------------
# Audio helpers
# ---------------------------------------------------------------------------

def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    """Run a subprocess, suppressing its output."""
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def is_audio_valid(path: str) -> bool:
    """
    Use ffprobe to verify the file contains at least one decodable audio
    stream.  Returns False for truncated M4A files missing the moov atom.
    """
    result = _run([
        "ffprobe",
        "-v", "error",
        "-select_streams", "a",
        "-show_entries", "stream=codec_type",
        "-of", "default=noprint_wrappers=1:nokey=1",
        path,
    ])
    return "audio" in result.stdout


def wait_for_valid_audio(path: str) -> bool:
    """
    Poll until the file:
      - exists on disk
      - has a stable size (fully written)
      - passes ffprobe audio-stream validation

    Returns True when all three conditions are met, False on timeout.
    """
    last_size = -1
    deadline  = time.monotonic() + FILE_READY_TIMEOUT

    while time.monotonic() < deadline:
        if not os.path.exists(path):
            log.debug("Waiting for file to appear: %s", path)
            time.sleep(FILE_POLL_INTERVAL)
            continue

        size = os.path.getsize(path)

        if size == 0:
            log.debug("File is empty, waiting…")
            time.sleep(FILE_POLL_INTERVAL)
            continue

        if size != last_size:
            # Still growing — keep waiting
            last_size = size
            time.sleep(FILE_POLL_INTERVAL)
            continue

        # Size has stabilised — check structural integrity
        if is_audio_valid(path):
            log.info("File ready (%d bytes): %s", size, path)
            return True

        log.debug("File size stable but audio stream invalid (moov atom missing?), retrying…")
        time.sleep(FILE_POLL_INTERVAL)

    log.warning("Timed out waiting for valid audio file: %s", path)
    return False


def convert_to_wav(input_path: str) -> str:
    """
    Transcode *input_path* to a 16 kHz mono WAV in a temp file.
    The caller is responsible for deleting the returned path.
    Raises RuntimeError if ffmpeg exits non-zero.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp.close()

    result = _run([
        "ffmpeg",
        "-y",
        "-i",    input_path,
        "-ar",   "16000",
        "-ac",   "1",
        "-f",    "wav",
        tmp.name,
    ])

    if result.returncode != 0:
        os.remove(tmp.name)
        raise RuntimeError(
            f"ffmpeg conversion failed for '{input_path}':\n{result.stderr.strip()}"
        )

    return tmp.name


def transcribe(file_path: str) -> dict:
    """
    Convert *file_path* to WAV then run Whisper.
    Retries up to MAX_RETRIES times with exponential back-off.
    Raises the last exception if all attempts fail.
    """
    last_exc: Exception | None = None
    delay = RETRY_DELAY

    for attempt in range(1, MAX_RETRIES + 1):
        wav_path = None
        try:
            log.info("[%d/%d] Converting to WAV…", attempt, MAX_RETRIES)
            wav_path = convert_to_wav(file_path)

            log.info("[%d/%d] Transcribing…", attempt, MAX_RETRIES)
            result = model.transcribe(wav_path, language=LANGUAGE, fp16=False)
            return result

        except Exception as exc:
            last_exc = exc
            log.warning("[%d/%d] Transcription error: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(delay)
                delay *= 2  # exponential back-off
        finally:
            if wav_path and os.path.exists(wav_path):
                os.remove(wav_path)

    raise RuntimeError(
        f"Transcription failed after {MAX_RETRIES} attempts"
    ) from last_exc


# ---------------------------------------------------------------------------
# Message handler
# ---------------------------------------------------------------------------
def handle_message(msg_value: dict) -> None:
    metadata      = msg_value["metadata"]
    payload       = msg_value["payload"]
    file_location = payload["file_location"]

    # --- DIAGNOSTIC ---
    exists = os.path.exists(file_location)
    size   = os.path.getsize(file_location) if exists else -1
    log.info("Message received | exists=%s | size=%d bytes | path=%s",
             exists, size, file_location)
    # ------------------

    if not os.path.exists(file_location):
        raise FileNotFoundError(f"Audio file not found: {file_location}")

    if not wait_for_valid_audio(file_location):
        raise RuntimeError(
            f"File never became a valid audio stream (moov atom missing?): {file_location}"
        )

    result = transcribe(file_location)
    text   = result["text"].strip()

    output = {
        "metadata": create_metadata(
            "transcription_agent",
            trace_id=metadata.get("trace_id", "unknown"),
        ),
        "payload": {
            "text":          text,
            "original_file": payload.get("file_name", "unknown"),
            "language":      result.get("language", LANGUAGE),
            "confidence":    result.get("confidence", 0.9),
        },
    }

    producer.send(OUTPUT_TOPIC, output)
    producer.flush()
    log.info("Published transcription (%d chars): %.120s…", len(text), text)

    try:
        os.remove(file_location)
        log.info("Garbage collection: Deleted processed file %s", file_location)
    except OSError as e:
        log.warning("Garbage collection failed for %s: %s", file_location, e)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
log.info("Transcription Agent listening on topic '%s'…", INPUT_TOPIC)

while True:
    try:
        batch = consumer.poll(timeout_ms=KAFKA_POLL_MS)

        for _tp, messages in batch.items():
            for msg in messages:
                try:
                    handle_message(msg.value)
                except KeyError as exc:
                    log.warning("Malformed message (missing key: %s)", exc)
                except Exception as exc:
                    log.error("Failed to process message: %s", exc)
                    dlq_message = {
                        "metadata": msg.value.get("metadata", {}),
                        "payload": msg.value.get("payload", {}),
                        "error": str(exc),
                        "failed_topic": INPUT_TOPIC,
                        "retry_count": msg.value.get("retry_count", 0),
                    }
                    try:
                        producer.send(DLQ_TOPIC, dlq_message)
                        producer.flush()
                        log.warning("Published failed message to DLQ: %s", DLQ_TOPIC)
                    except Exception as dlq_exc:
                        log.error("Failed to publish to DLQ: %s", dlq_exc)

    except Exception as kafka_exc:
        log.error("Kafka poll error: %s — retrying in 2s", kafka_exc)
        time.sleep(2)