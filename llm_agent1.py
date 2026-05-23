import json
import logging
import os

from dotenv import load_dotenv
from groq import Groq
from kafka import KafkaConsumer, KafkaProducer

from utils import create_metadata

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("fact_check_agent")


client = Groq(api_key=os.getenv("GROQ_API_KEY"))

PROMPT = """
You are an expert fact-checker.
Your task is to classify the given claim strictly as either:
- "true"
- "false"

You must return ONLY valid JSON in the following format:
{
    "classification": "true" or "false",
    "explanation": "<short explanation>"
}
"""

consumer = KafkaConsumer(
    "analysis.claims",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

DLQ_TOPIC = os.getenv("DLQ_TOPIC", "analysis.claims.dlq")

for msg in consumer:
    metadata = msg.value["metadata"]
    claims = msg.value["payload"]["claims"]

    for claim in claims:
        try:
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": PROMPT},
                    {"role": "user", "content": claim},
                ],
                temperature=0,
            )

            clean = response.choices[0].message.content.replace("```", "").strip()
            result = json.loads(clean)

            output = {
                "metadata": create_metadata(
                    "groq_agent",
                    trace_id=metadata["trace_id"],
                ),
                "payload": {
                    "claim": claim,
                    **result,
                },
            }

            producer.send("analysis.misinformation.raw", output)
            producer.flush()
            log.info("Published fact-check result for trace_id=%s", metadata.get("trace_id"))
        except Exception as exc:
            log.error("Fact-check processing failed: %s", exc)
            dlq_message = {
                "metadata": msg.value.get("metadata", {}),
                "payload": msg.value.get("payload", {}),
                "error": str(exc),
                "failed_topic": "analysis.claims",
                "retry_count": msg.value.get("retry_count", 0),
            }
            try:
                producer.send(DLQ_TOPIC, dlq_message)
                producer.flush()
                log.warning("Published failed message to DLQ: %s", DLQ_TOPIC)
            except Exception as dlq_exc:
                log.error("Failed to publish to DLQ: %s", dlq_exc)