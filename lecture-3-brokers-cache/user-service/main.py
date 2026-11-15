import json
import os
from pathlib import Path
from uuid import uuid4

from confluent_kafka import KafkaException, Producer
from flask import Flask, jsonify, send_from_directory


ROOT = Path(__file__).resolve().parent

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
})

app = Flask(__name__, static_folder=None)


@app.get("/")
def index():
    return send_from_directory(ROOT, "index.html")


@app.get("/styles.css")
def styles():
    return send_from_directory(ROOT, "styles.css")


@app.post("/api/orders")
def create_order():
    order_id = f"order-{uuid4().hex[:8]}"

    event = {
        "order_id": order_id,
        "items": ["tea", "cookies"],
    }

    delivery = {}

    def delivery_report(error, message):
        if error is not None:
            delivery["error"] = str(error)
            return

        delivery["partition"] = message.partition()
        delivery["offset"] = message.offset()

    try:
        producer.produce(
            topic=KAFKA_TOPIC,
            key=order_id,
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )

        remaining_messages = producer.flush(10)

    except (BufferError, KafkaException) as error:
        return jsonify(
            message=f"Не удалось отправить заказ: {error}",
        ), 503

    if remaining_messages > 0:
        return jsonify(
            message="Kafka не подтвердила отправку за 10 секунд.",
        ), 503

    if "error" in delivery:
        return jsonify(
            message=f"Kafka отклонила сообщение: {delivery['error']}",
        ), 503

    partition = delivery["partition"]
    offset = delivery["offset"]

    return jsonify(
        order_id=order_id,
        partition=partition,
        offset=offset,
        message=(
            f"Заказ отправлен в Kafka: "
            f"partition {partition}, offset {offset}."
        ),
    ), 201


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)