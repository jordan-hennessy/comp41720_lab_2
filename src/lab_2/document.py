import json
import sqlite3
import threading
import time
import uuid
from contextlib import asynccontextmanager, closing
from pathlib import Path

import httpx
import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from lab_2.config import (
    DOCUMENT_STORED_QUEUE,
    EMBEDDING_RESULTS_QUEUE,
    QUOTA_URL,
    RABBITMQ_HOST,
)

# --- Settings ---

DB_PATH = Path(__file__).resolve().parents[2] / "document.db"


# --- Database ---


def connect() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, timeout=10)


def init_db() -> None:
    with closing(connect()) as db, db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS documents ("
            "id TEXT PRIMARY KEY, tenant_id TEXT, text TEXT, status TEXT)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS outbox ("
            "message_id TEXT PRIMARY KEY, payload TEXT, published INTEGER DEFAULT 0)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS processed_messages (message_id TEXT PRIMARY KEY)"
        )


# --- Quota service calls ---


def quota_call(action: str, tenant_id: str, document_id: str) -> None:
    response = httpx.post(
        f"{QUOTA_URL}/quota/{action}",
        json={"tenant_id": tenant_id, "document_id": document_id},
        timeout=5.0,
    )
    response.raise_for_status()


# --- Outbox relay ---
# Publishes outbox rows to RabbitMQ, then marks them published. Publish first,
# mark second: a crash in between causes a duplicate, never a lost event.


def run_relay() -> None:
    while True:
        with closing(connect()) as db:
            rows = db.execute(
                "SELECT message_id, payload FROM outbox WHERE published = 0"
            ).fetchall()

        if rows:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(RABBITMQ_HOST)
            )
            channel = connection.channel()
            channel.queue_declare(queue=DOCUMENT_STORED_QUEUE, durable=True)

            for message_id, payload in rows:
                channel.basic_publish(
                    exchange="",
                    routing_key=DOCUMENT_STORED_QUEUE,
                    body=payload,
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                with closing(connect()) as db, db:
                    db.execute(
                        "UPDATE outbox SET published = 1 WHERE message_id = ?",
                        (message_id,),
                    )
                print(f"Relay published {message_id}")

            connection.close()

        time.sleep(0.1)


# --- Models ---


class DocumentRequest(BaseModel):
    tenant_id: str = "default"
    text: str


# --- Result consumer ---
# Listens for the Embedding service's result, then completes or compensates the saga.


def on_result(ch, method, properties, body):
    event = json.loads(body)
    message_id = event["message_id"]
    document_id = event["document_id"]
    print(f"Result {event['type']} for {document_id}")

    # Idempotency guard: ignore events we have already handled.
    with closing(connect()) as db:
        seen = db.execute(
            "SELECT 1 FROM processed_messages WHERE message_id = ?", (message_id,)
        ).fetchone()
    if seen:
        print(f"Duplicate {message_id}, ignoring")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    if event["type"] == "embedded":
        status = "COMPLETED"
    else:
        # Compensation: give the reserved quota back, then mark the document FAILED.
        with closing(connect()) as db:
            row = db.execute(
                "SELECT tenant_id FROM documents WHERE id = ?", (document_id,)
            ).fetchone()
        quota_call("release", row[0], document_id)
        status = "FAILED"

    # Save the new status and the "handled" marker together.
    with closing(connect()) as db, db:
        db.execute(
            "UPDATE documents SET status = ? WHERE id = ?", (status, document_id)
        )
        db.execute(
            "INSERT OR IGNORE INTO processed_messages (message_id) VALUES (?)",
            (message_id,),
        )
    print(f"{document_id} -> {status}")

    ch.basic_ack(delivery_tag=method.delivery_tag)  # ack only after everything is saved


def run_result_consumer() -> None:
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=EMBEDDING_RESULTS_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=EMBEDDING_RESULTS_QUEUE, on_message_callback=on_result)
    channel.start_consuming()


# --- App ---


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    threading.Thread(target=run_relay, daemon=True).start()
    threading.Thread(target=run_result_consumer, daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)


# --- Routes ---


@app.post("/documents", status_code=202)
def create_document(req: DocumentRequest) -> dict:
    document_id = str(uuid.uuid4())

    # Saga step 1: reserve quota.
    quota_call("reserve", req.tenant_id, document_id)

    # Saga step 2: save the document AND the event in ONE transaction
    # (the outbox pattern), so we can never have one without the other.
    event = {
        "message_id": str(uuid.uuid4()),
        "document_id": document_id,
        "tenant_id": req.tenant_id,
        "text": req.text,
    }
    with closing(connect()) as db, db:
        db.execute(
            "INSERT INTO documents (id, tenant_id, text, status) VALUES (?, ?, ?, 'PENDING')",
            (document_id, req.tenant_id, req.text),
        )
        db.execute(
            "INSERT INTO outbox (message_id, payload) VALUES (?, ?)",
            (event["message_id"], json.dumps(event)),
        )

    return {"id": document_id}


@app.get("/documents/{document_id}/status")
def get_status(document_id: str) -> dict:
    with closing(connect()) as db:
        row = db.execute(
            "SELECT status FROM documents WHERE id = ?", (document_id,)
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Unknown document ID")
    return {"id": document_id, "status": row[0]}
