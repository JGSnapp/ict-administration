import json
import logging
import sys

from flask import Flask, jsonify, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, stream=sys.stdout, force=True)
log = logging.getLogger("alert-webhook")


@app.post("/alerts")
def alerts():
    payload = request.get_json(silent=True)
    if payload is None:
        return jsonify(error="expected JSON"), 400

    log.info("Alertmanager notification: %s", json.dumps(payload, ensure_ascii=False))
    return jsonify(status="received"), 200


@app.get("/health")
def health():
    return jsonify(status="ok")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
