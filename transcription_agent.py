import json
import base64
import tempfile
import os
import signal
import sys
from kafka import KafkaConsumer, KafkaProducer
import whisper
from utils import create_metadata

model = whisper.load_model("base")

consumer = KafkaConsumer(
    "audio.raw",
    bootstrap_servers="localhost:9092",
    group_id="transcription-agent",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    max_partition_fetch_bytes=10 * 1024 * 1024
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    max_request_size=10 * 1024 * 1024
)

def shutdown(sig, frame):
    print("\n Shutting down gracefully...")
    consumer.close()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)   
signal.signal(signal.SIGTERM, shutdown)  


for msg in consumer:
    try:
        metadata = msg.value["metadata"]
        payload = msg.value["payload"]

        audio_bytes = base64.b64decode(payload["audio_data"])

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp:
                tmp.write(audio_bytes)
                tmp_path = tmp.name

            result = model.transcribe(tmp_path)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

        output = {
            "metadata": create_metadata(
                "transcription_agent",
                trace_id=metadata["trace_id"]
            ),
            "payload": {
                "text": result["text"],
                "language": result.get("language", "unknown"),
                "confidence": result.get("confidence", 0.9)
            }
        }

        producer.send("audio.transcribed", output)
        producer.flush()
        print(f"Transcribed: {result['text'][:80]}...")

    except Exception as e:
        print(f"Failed to process message: {e}")