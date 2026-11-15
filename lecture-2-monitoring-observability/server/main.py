"""Подопытный сервис: три кнопки — ошибка, задержка и нагрузка."""
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import json
import logging
import sys

import requests
from flask import Flask, jsonify, request, send_from_directory, Response, g
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from opentelemetry import trace
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace import Status, StatusCode

ROOT = Path(__file__).resolve().parent
PORT = int(os.getenv('PORT', '5000'))
app = Flask(__name__, static_folder=None)

provider = TracerProvider(
    resource=Resource.create({
        "service.name": os.getenv("OTEL_SERVICE_NAME", "lab-service")
    })
)

provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter())
)

trace.set_tracer_provider(provider)

FlaskInstrumentor().instrument_app(
    app,
    excluded_urls="metrics,styles.css",
)

tracer = trace.get_tracer(__name__)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        context = trace.get_current_span().get_span_context()
        return json.dumps({
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "trace_id": (
                format(context.trace_id, "032x")
                if context.is_valid
                else None
            ),
        }, ensure_ascii=False)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())

logging.basicConfig(
    level=logging.INFO,
    handlers=[handler],
    force=True,
)

log = logging.getLogger("lab-service")

REQUESTS = Counter(
    'http_requests_total',
    'Total number of HTTP requests',
    ['method'],
)

for method in ("GET", "POST", "other"):
    REQUESTS.labels(method=method)

ERRORS = Counter(
    'http_errors_total',
    'Total number of HTTP 5xx responses',
)

DURATION = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    buckets=(0.01, 0.05, 0.1, 0.5, 1, 2, 3, 5),
)

@app.get('/')
def index():
    return send_from_directory(ROOT, 'index.html')


@app.get('/styles.css')
def styles():
    return send_from_directory(ROOT, 'styles.css')


@app.get('/api/ping')
def ping():
    return jsonify(message='Сервис работает')


@app.post('/api/error')
def error():
    trace.get_current_span().set_status(
        Status(StatusCode.ERROR, "Intentional HTTP 500")
    )
    return jsonify(message='Учебная ошибка: HTTP 500'), 500


@app.post('/api/slow')
def slow():
    with tracer.start_as_current_span("slow-dependency"):
        time.sleep(2)
    return jsonify(message='Медленная операция завершена за 2 секунды')


@app.post('/api/load')
def load():
    # Фиксированный локальный адрес: пользователь не может задать внешний target.
    target = f'http://127.0.0.1:{PORT}/api/ping'
    def hit(_):
        try:
            return requests.get(target, timeout=5).status_code == 200
        except requests.RequestException:
            return False
    with ThreadPoolExecutor(max_workers=10) as pool:
        success = sum(pool.map(hit, range(100)))
    return jsonify(message=f'Нагрузка: успешно {success} из 100 запросов',
                   successful=success, total=100), 200 if success == 100 else 503

@app.before_request
def start_timer():
    g.started = time.perf_counter()

@app.after_request
def count_request(response):
    if request.path != '/metrics':
        elapsed = time.perf_counter() - g.started
        DURATION.observe(elapsed)
        if request.method=='GET':
            REQUESTS.labels(method='GET').inc()
        elif request.method=='POST':
            REQUESTS.labels(method='POST').inc()
        else:
            REQUESTS.labels(method='other').inc()
        if response.status_code >= 500:
            ERRORS.inc()

        log.log(
            logging.ERROR if response.status_code >= 500 else logging.INFO,
            "%s %s status=%s duration=%.3fs",
            request.method,
            request.path,
            response.status_code,
            elapsed,
        )
    return response


@app.get('/metrics')
def metrics():
    return Response(
        generate_latest(),
        content_type=CONTENT_TYPE_LATEST,
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, threaded=True, debug=False)
