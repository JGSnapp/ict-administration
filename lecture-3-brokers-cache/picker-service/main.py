"""Минимальный сервис сборщика без подключения к Kafka."""

import os
from pathlib import Path

from flask import Flask, send_from_directory


ROOT = Path(__file__).resolve().parent
INSTANCE_NAME = os.getenv("INSTANCE_NAME", "picker-1")
app = Flask(__name__, static_folder=None)


@app.get("/")
def index():
    print(f"[{INSTANCE_NAME}] Kafka is not connected; waiting for orders.", flush=True)
    return send_from_directory(ROOT, "index.html")


@app.get("/styles.css")
def styles():
    return send_from_directory(ROOT, "styles.css")


if __name__ == "__main__":
    print(f"[{INSTANCE_NAME}] Picker service started.", flush=True)
    app.run(host="0.0.0.0", port=5001, debug=False)
