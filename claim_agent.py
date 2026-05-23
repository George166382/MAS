"""
Claim Extraction Agent
----------------------
Consumes transcribed text from Kafka, extracts factual claims via Groq/LLaMA,
and publishes structured results.

Handles multilingual transcriptions by instructing the LLM to extract and
translate claims into English regardless of the source language.
"""

import json
import logging
import os
import re
import signal
import sys
import uuid

from dotenv import load_dotenv
from groq import Groq
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
log = logging.getLogger("claim_agent")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP",  "localhost:9092")
INPUT_TOPIC     = os.getenv("INPUT_TOPIC",       "audio.transcribed")
OUTPUT_TOPIC    = os.getenv("OUTPUT_TOPIC",       "analysis.claims")
DLQ_TOPIC       = os.getenv("DLQ_TOPIC",         "audio.transcribed.dlq")
GROQ_MODEL      = os.getenv("GROQ_MODEL",        "llama-3.1-8b-instant")

# ---------------------------------------------------------------------------
# Groq client
# ---------------------------------------------------------------------------
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """
You are a multilingual fact-extraction engine.

Your job:
- Read the user's text (it may be in ANY language).
- Identify every distinct factual claim — statements presented as facts that
  could be verified as true or false.
- Translate each claim into English if it is not already in English.
- Return ONLY a valid JSON object. No markdown, no explanation, nothing else.

Output format (strictly):
{
  "claims": [
    "claim in English",
    "another claim in English"
  ]
}

If no verifiable factual claims are present, return:
{
  "claims": []
}
""".strip()

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=f"claim-agent-{uuid.uuid4()}",
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
    log.info("Signal %s received -- shutting down.", sig)
    consumer.close()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_claims(text: str) -> list[str]:
    """
    Call the LLM and parse the JSON response.
    Returns a list of claim strings.
    Raises ValueError if the response cannot be parsed.
    """
    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": text},
        ],
        temperature=0,
    )

    raw = response.choices[0].message.content
    log.debug("LLM raw response: %s", raw)

    # Strip optional markdown fences
    clean = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()

    try:
        parsed = json.loads(clean)
    except json.JSONDecodeError as exc:
        raise ValueError(f"LLM returned invalid JSON: {exc}\nRaw:\n{raw}") from exc

    claims = parsed.get("claims", [])

    if not isinstance(claims, list):
        raise ValueError(f"Expected 'claims' to be a list, got: {type(claims)}\nRaw:\n{raw}")

    return claims


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
log.info("Claim Agent listening on topic '%s'...", INPUT_TOPIC)

for msg in consumer:
    raw_llm = None
    try:
        metadata = msg.value["metadata"]
        text     = msg.value["payload"]["text"]

        log.info("Processing transcription (trace_id=%s): %.120s",
                 metadata.get("trace_id", "?"), text)

        claims = extract_claims(text)

        if not claims:
            log.warning(
                "LLM returned 0 claims for text: '%s' — "
                "check if the text contains verifiable facts.",
                text[:200],
            )

        output = {
            "metadata": create_metadata(
                "claim_agent",
                trace_id=metadata["trace_id"],
            ),
            "payload": {
                "claims":        claims,
                "source_text":   text,
                "claims_count":  len(claims),
            },
        }

        producer.send(OUTPUT_TOPIC, output)
        producer.flush()
        log.info("Published %d claim(s): %s", len(claims), claims)

    except KeyError as exc:
        log.warning("Skipping malformed message (missing key: %s)", exc)
    except ValueError as exc:
        log.error("Claim extraction failed: %s", exc)
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
    except Exception as exc:
        log.error("Unexpected error processing message: %s", exc)
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