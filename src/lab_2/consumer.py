import json
import os

import httpx
import pika

from lab_1.producer import QUEUE_NAME, RABBITMQ_HOST, declare_topology, save_result

MAX_RETRIES = 3

# Overridable so the AI dependency can be broken without editing code, e.g.
#   AI_URL=http://localhost:9999/api/generate python -m lab_1.consumer
AI_URL = os.environ.get("AI_URL", "http://localhost:11434/api/generate")
AI_MODEL = os.environ.get("AI_MODEL", "llama3.2:1b")


def dead_letter(channel, method, request_id: str | None, reason: str) -> None:
    """Record the failure (if we know the request) and route to the DLQ."""
    if request_id:
        save_result(request_id, {"id": request_id, "status": "error", "error": reason})
    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def on_message(channel, method, properties, body: bytes) -> None:
    # A message we can't parse will never succeed, so dead-letter it straight
    # away. Raising here would crash the consumer and loop on redelivery.
    try:
        message = json.loads(body)
        request_id = message["id"]
        text = message["text"]
    except (ValueError, KeyError, TypeError) as e:
        print(f"Malformed message — routing to DLQ: {e!r}")
        request_id = None
        try:
            request_id = json.loads(body).get("id")
        except (ValueError, AttributeError):
            pass
        dead_letter(channel, method, request_id, f"malformed message: {e!r}")
        return

    retry_count = (properties.headers or {}).get("x-retry-count", 0)

    try:
        response = httpx.post(
            AI_URL,
            json={"model": AI_MODEL, "prompt": text, "stream": False},
            timeout=60.0,
        )
        response.raise_for_status()
        ai_output = response.json()["response"]

        save_result(
            request_id, {"id": request_id, "status": "completed", "result": ai_output}
        )
        print(f"Completed {request_id}")
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except (httpx.HTTPError, ValueError, KeyError) as e:
        # HTTPError: timeout/connection/5xx. ValueError/KeyError: the AI replied
        # with something unusable. All are treated as a failed attempt.
        if retry_count < MAX_RETRIES - 1:
            print(
                f"AI call failed for {request_id} (attempt {retry_count + 1}/{MAX_RETRIES}): {e!r} — retrying"
            )
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=body,  # same payload, unchanged
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type="application/json",
                    headers={"x-retry-count": retry_count + 1},
                ),
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            print(
                f"AI call failed for {request_id} after {MAX_RETRIES} attempts: {e!r} — routing to DLQ"
            )
            dead_letter(channel, method, request_id, str(e) or repr(e))


def main() -> None:
    # The AI call blocks pika's I/O loop, so allow a long heartbeat or the
    # broker may drop the connection during a slow model response.
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600)
    )
    channel = connection.channel()
    declare_topology(channel)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False
    )
    print(f"Waiting for messages on '{QUEUE_NAME}'. Press Ctrl+C to exit.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
