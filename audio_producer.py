import json
import os
from kafka import KafkaProducer
from utils import create_metadata

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
    
)

def send_audio(file_path: str) -> str:   # now returns trace_id
    file_ext = os.path.splitext(file_path)[1].replace(".", "")

    metadata = create_metadata("audio_producer")  # trace_id is born here
    trace_id = metadata["trace_id"]               # pull it out before sending

    message = {
        "metadata": metadata,
        "payload": {
            "file_name":     os.path.basename(file_path),
            "file_location": os.path.abspath(file_path),
            "format":        file_ext
        }
    }

    future = producer.send("audio.raw", message)
    producer.flush()

    record = future.get(timeout=10)
    print(f"Sent '{file_path}' → partition {record.partition}, offset {record.offset}, trace_id={trace_id}")

    return trace_id   # ← server needs this to respond to Android
if __name__ == "__main__":
    send_audio("audio/audio.wav")