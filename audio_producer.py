# audio_producer.py
import json
import base64
from kafka import KafkaProducer
from utils import create_metadata

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    max_request_size=10 * 1024 * 1024,   
    buffer_memory=10 * 1024 * 1024       
)

def send_audio(file_path: str) -> None:
    with open(file_path, "rb") as f:
        audio_bytes = f.read()

    message = {
        "metadata": create_metadata("audio_producer"),
        "payload": {
            "file_name": file_path,
            "audio_data": base64.b64encode(audio_bytes).decode("utf-8"),
            "format": "wav"
        }
    }

    future = producer.send("audio.raw", message)
    producer.flush()

    record = future.get(timeout=10)
    print(f"Sent '{file_path}' → partition {record.partition}, offset {record.offset}")

if __name__ == "__main__":
    send_audio("audio/audio.wav")