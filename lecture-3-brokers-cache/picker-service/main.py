"""Сервис сборщика: читает и обрабатывает заказы из Kafka."""

import json
import os
import threading
import time
from pathlib import Path

from confluent_kafka import Consumer
from flask import Flask, jsonify, send_from_directory


ROOT = Path(__file__).resolve().parent

INSTANCE_NAME = os.getenv("INSTANCE_NAME", "picker-1")
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "pickers")

app = Flask(__name__, static_folder=None)

state_lock = threading.Lock()
latest_state = {
    "status": "waiting",
    "instance": INSTANCE_NAME,
}


def consume_orders():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([KAFKA_TOPIC])

    print(
        f"[{INSTANCE_NAME}] Subscribed to topic {KAFKA_TOPIC} "
        f"as group {KAFKA_GROUP_ID}.",
        flush=True,
    )

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(
                    f"[{INSTANCE_NAME}] Consumer error: {message.error()}",
                    flush=True,
                )
                continue

            try:
                order = json.loads(message.value().decode("utf-8"))

                # Условная сборка заказа.
                time.sleep(1)

                # Offset фиксируется только после обработки.
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                with state_lock:
                    latest_state.clear()
                    latest_state.update({
                        "status": "processed",
                        "instance": INSTANCE_NAME,
                        "order_id": order.get("order_id"),
                        "items": order.get("items", []),
                        "partition": message.partition(),
                        "offset": message.offset(),
                    })

                print(
                    f"[{INSTANCE_NAME}] Processed "
                    f"order={order.get('order_id')} "
                    f"partition={message.partition()} "
                    f"offset={message.offset()}",
                    flush=True,
                )

            except Exception as error:
                print(
                    f"[{INSTANCE_NAME}] Processing error: {error}",
                    flush=True,
                )

    finally:
        consumer.close()


@app.get("/")
def index():
    return send_from_directory(ROOT, "index.html")


@app.get("/styles.css")
def styles():
    return send_from_directory(ROOT, "styles.css")


@app.get("/api/status")
def status():
    with state_lock:
        return jsonify(dict(latest_state))


if __name__ == "__main__":
    consumer_thread = threading.Thread(
        target=consume_orders,
        name="kafka-consumer",
        daemon=True,
    )
    consumer_thread.start()

    app.run(host="0.0.0.0", port=5001, threaded=True, debug=False)