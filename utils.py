import uuid
from datetime import datetime

def create_metadata(source, trace_id=None):
    return {
        "message_id": str(uuid.uuid4()),
        "trace_id": trace_id or str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "source": source
    }