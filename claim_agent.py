import json
import os
import signal
import sys
import re
from kafka import KafkaConsumer, KafkaProducer
from groq import Groq
from utils import create_metadata
from dotenv import load_dotenv
import os

load_dotenv()  


client = Groq(api_key=os.getenv("GROQ_API_KEY"))

PROMPT = """
Extract factual claims from the text.
Return ONLY valid JSON with no markdown, no explanation:
{
  "claims": ["claim1", "claim2"]
}
"""

consumer = KafkaConsumer(
    "audio.transcribed",
    bootstrap_servers="localhost:9092",
    group_id="claim-agent",                                   
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def shutdown(sig, frame):
    print("\n Shutting down ...")
    consumer.close()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

def extract_json(text: str) -> dict:
    """Strip markdown fences and extract JSON robustly."""
    # Remove ```json ... ``` or ``` ... ```
    clean = re.sub(r"```(?:json)?", "", text).strip()
    return json.loads(clean)


for msg in consumer:
    try:
        metadata = msg.value["metadata"]
        text = msg.value["payload"]["text"]

        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": PROMPT},
                {"role": "user", "content": text}
            ],
            temperature=0
        )

        raw = response.choices[0].message.content
        claims = extract_json(raw)                             

        output = {
            "metadata": create_metadata(
                "claim_agent",
                trace_id=metadata["trace_id"]
            ),
            "payload": claims
        }

        producer.send("analysis.claims", output)
        producer.flush()                                       
        print(f"Extracted {len(claims.get('claims', []))} claims")

    except json.JSONDecodeError as e:
        print(f"LLM returned invalid JSON: {e}\nRaw: {raw}")
    except Exception as e:
        print(f"Failed to process message: {e}")