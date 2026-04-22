from kafka import KafkaConsumer, KafkaProducer
import json, os
from groq import Groq
from utils import create_metadata
from dotenv import load_dotenv
import os

load_dotenv() 


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
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": PROMPT},
                {"role": "user", "content": claim}
            ],
            temperature=0
        )

        clean = response.choices[0].message.content.replace("```", "").strip()
        result = json.loads(clean)

        output = {
            "metadata": create_metadata(
                "groq_agent",
                trace_id=metadata["trace_id"]
            ),
            "payload": {
                "claim": claim,
                **result
            }
        }

        producer.send("analysis.misinformation.raw", output)