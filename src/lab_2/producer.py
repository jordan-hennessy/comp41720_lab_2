import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

RABBITMQ_HOST = "localhost"
QUEUE_NAME = "task_queue"
# Anchored to the project root so producer and consumer share one store
# regardless of the directory they are launched from.
RESULTS_DIR = Path(__file__).resolve().parents[2] / "results"

DLX_NAME = "task_dlx"
DLQ_NAME = "task_dlq"

app = FastAPI()


class ProcessRequest(BaseModel):
    text: str


# --- Results store: one JSON file per request, shared by producer and consumer ---


def save_result(request_id: str, data: dict) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / f"{request_id}.json"
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    tmp.replace(path)  # atomic swap, so a reader never sees a half-written file


def load_result(request_id: str) -> dict | None:
    path = RESULTS_DIR / f"{request_id}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


# --- RabbitMQ ---


def publish(payload: dict) -> None:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(payload).encode(),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
    finally:
        connection.close()


def declare_topology(channel):
    channel.exchange_declare(exchange=DLX_NAME, exchange_type="direct", durable=True)
    channel.queue_declare(queue=DLQ_NAME, durable=True)
    channel.queue_bind(queue=DLQ_NAME, exchange=DLX_NAME, routing_key=DLQ_NAME)
    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX_NAME,
            "x-dead-letter-routing-key": DLQ_NAME,
        },
    )


# --- Routes ---


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/process")
def process(req: ProcessRequest) -> dict:
    request_id = str(uuid.uuid4())
    payload = {
        "id": request_id,
        "text": req.text,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    # Record "processing" BEFORE publishing, or a fast consumer could write
    # "completed" first and then have it overwritten.
    save_result(request_id, {"id": request_id, "status": "processing"})
    try:
        publish(payload)
    except pika.exceptions.AMQPError: # type: ignore
        save_result(
            request_id,
            {"id": request_id, "status": "error", "error": "broker unavailable"},
        )
        raise HTTPException(status_code=503, detail="Message broker unavailable")
    return {"id": request_id}


@app.get("/result/{request_id}")
def get_result(request_id: str) -> dict:
    try:
        uuid.UUID(request_id)  # reject anything that isn't a valid ID, e.g. "../secret"
    except ValueError:
        raise HTTPException(status_code=404, detail="Unknown request ID")
    result = load_result(request_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Unknown request ID")
    return result
