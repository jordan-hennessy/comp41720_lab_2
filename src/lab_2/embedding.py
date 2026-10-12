import hashlib
import json
import random
import threading
import time
from contextlib import asynccontextmanager

import pika
from fastapi import FastAPI

from lab_2.config import (
    DOCUMENT_STORED_QUEUE,
    EMBEDDING_RESULTS_QUEUE,
    FAILURE_RATE,
    RABBITMQ_HOST,
)

# --- Settings ---

# Message IDs already handled (idempotency guard). In memory, so it resets on restart.
handled = set()


# --- Embedder ---


def embed(text: str) -> list[int]:
    """Fake embedder: turns the text into 8 numbers."""
    time.sleep(0.2)
    if random.random() < FAILURE_RATE:
        raise RuntimeError("embedding failed")
    return list(hashlib.sha256(text.encode()).digest()[:8])


# --- Consumer ---


def on_message(ch, method, properties, body):
    event = json.loads(body)
    print(f"Received {event['document_id']}")

    # A redelivered event must not be embedded twice.
    if event["message_id"] in handled:
        print(f"Duplicate {event['message_id']}, ignoring")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # A failed embedding is reported as an event, not a crash, so the Document service can compensate.
    try:
        embed(event["text"])
        result_type = "embedded"
    except RuntimeError:
        result_type = "embedding_failed"

    result = {
        "message_id": event["message_id"],
        "document_id": event["document_id"],
        "type": result_type,
    }
    ch.basic_publish(
        exchange="",
        routing_key=EMBEDDING_RESULTS_QUEUE,
        body=json.dumps(result),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print(f"Published {result_type} for {event['document_id']}")

    handled.add(event["message_id"])

    # Ack only after publishing, so a crash means redelivery, not loss.
    ch.basic_ack(delivery_tag=method.delivery_tag)


def run_consumer() -> None:
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=DOCUMENT_STORED_QUEUE, durable=True)
    channel.queue_declare(queue=EMBEDDING_RESULTS_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=DOCUMENT_STORED_QUEUE, on_message_callback=on_message)
    print("Waiting for events...")
    channel.start_consuming()


# --- App ---


@asynccontextmanager
async def lifespan(app: FastAPI):
    # The consumer blocks forever, so it runs in a background thread.
    threading.Thread(target=run_consumer, daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)

# --- Routes ---


@app.get("/embedding/health")
def health() -> dict:
    return {"status": "ok"}
