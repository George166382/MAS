from kafka import KafkaConsumer, KafkaProducer
import json, os
import cohere
from utils import create_metadata

client = cohere.Client(os.getenv("COHERE_API_KEY"))

PROMPT = """You are an expert fact-checker.
Your task is to classify the given claim strictly as either:
- "true"
- "false"

You must return ONLY valid JSON in the following format:
{
    "classification": "true" or "false",
    "explanation": "<short explanation>"
}"""

consumer = KafkaConsumer(
    "analysis.claims",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for msg in consumer:
    metadata = msg.value["metadata"]
    claims = msg.value["payload"]["claims"]

    for claim in claims:
        response = client.chat(
            message=claim,
            preamble=PROMPT,
            model="command-r-08-2024",
            temperature=0
        )

        clean = response.text.replace("```", "").strip()
        result = json.loads(clean)

        output = {
            "metadata": create_metadata(
                "cohere_agent",
                trace_id=metadata["trace_id"]
            ),
            "payload": {
                "claim": claim,
                **result
            }
        }

        producer.send("analysis.misinformation.raw", output)