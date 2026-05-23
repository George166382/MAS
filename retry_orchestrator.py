"""
retry_orchestrator.py
---------------------
Subscribes to all DLQ topics and replays messages with exponential backoff.
After MAX_RETRIES_TOTAL, marks the trace_id as FAILED in the Result DB.
"""

import json
import logging
import os
import signal
import sys
import time

import result_store
from kafka import KafkaConsumer, KafkaProducer
from result_store import Status

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("retry_orchestrator")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MAX_RETRIES_TOTAL = int(os.getenv("MAX_RETRIES_TOTAL", "3"))
BASE_DELAY = float(os.getenv("BASE_RETRY_DELAY", "5.0"))

DLQ_TO_ORIGIN = {
    "audio.raw.dlq": "audio.raw",
    "audio.transcribed.dlq": "audio.transcribed",
    "analysis.claims.dlq": "analysis.claims",
}

consumer = KafkaConsumer(
    *DLQ_TO_ORIGIN.keys(),
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="retry-orchestrator",
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def _shutdown(sig, frame):
    log.info("Shutting down retry orchestrator…")
    consumer.close()
    producer.close()
    sys.exit(0)


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

log.info("Retry Orchestrator listening on DLQ topics: %s", list(DLQ_TO_ORIGIN))

for msg in consumer:
    dlq_topic = msg.topic
    origin_topic = DLQ_TO_ORIGIN.get(dlq_topic)
    value = msg.value
    trace_id = value.get("metadata", {}).get("trace_id", "unknown")
    retry_count = int(value.get("retry_count", 0)) + 1
    error = value.get("error", "unknown error")

    log.warning(
        "DLQ message received | trace_id=%s | retry=%d/%d | topic=%s | error=%s",
        trace_id,
        retry_count,
        MAX_RETRIES_TOTAL,
        dlq_topic,
        error,
    )

    if retry_count > MAX_RETRIES_TOTAL:
        log.error("Max retries exceeded for trace_id=%s - marking FAILED", trace_id)
        result_store.update(
            trace_id,
            Status.FAILED,
            error=f"Exceeded {MAX_RETRIES_TOTAL} retries. Last error: {error}",
        )
        continue

    delay = BASE_DELAY * (2 ** (retry_count - 1))
    log.info(
        "Waiting %.1fs before re-enqueuing trace_id=%s to '%s'",
        delay,
        trace_id,
        origin_topic,
    )
    time.sleep(delay)

    replay = {
        "metadata": value.get("metadata", {}),
        "payload": value.get("payload", {}),
        "retry_count": retry_count,
    }
    producer.send(origin_topic, replay)
    producer.flush()
    log.info("Re-enqueued trace_id=%s to '%s' (attempt %d)", trace_id, origin_topic, retry_count)