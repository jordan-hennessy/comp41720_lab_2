"""Publish a message the consumer can never process (no "text" field).

It is dead-lettered immediately, without needing Ollama to be down. The result
is recorded as "processing" first so GET /result/{id} works for it.
"""

import uuid

from lab_2.producer import publish, save_result

if __name__ == "__main__":
    request_id = str(uuid.uuid4())
    save_result(request_id, {"id": request_id, "status": "processing"})
    publish({"id": request_id})
    print(f"Poison message sent: {request_id}")
