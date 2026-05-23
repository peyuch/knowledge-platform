"""Index consumer process entry point.

Starts the Kafka consumer loop + HTTP health/metrics server.

Usage:
    python -m workers.tasks.embed
    python -m workers.tasks.embed --batch-size 200 --batch-timeout 3
"""

import argparse
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from core.config import settings
from services.indexing.consumer import IndexConsumer


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self._respond(200, "OK")
        elif self.path == "/ready":
            self._respond(200, "OK")  # Stub; in prod: check ES/Milvus connections
        elif self.path == "/metrics":
            body = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self._respond(404, "Not Found")

    def _respond(self, status: int, body: str):
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, format, *args):
        pass  # Suppress HTTP access logs


def main():
    parser = argparse.ArgumentParser(description="Index Consumer")
    parser.add_argument("--batch-size", type=int, help="Batch size for flushing")
    parser.add_argument("--batch-timeout", type=float, help="Batch timeout (seconds)")
    parser.add_argument("--buffer-max", type=int, help="Buffer max size for backpressure")
    parser.add_argument("--kafka-group", type=str, help="Kafka consumer group ID")
    args = parser.parse_args()

    if args.batch_size:
        settings.index_batch_size = args.batch_size
    if args.batch_timeout:
        settings.index_batch_timeout = args.batch_timeout
    if args.buffer_max:
        settings.index_buffer_max_size = args.buffer_max
    if args.kafka_group:
        settings.index_kafka_group_id = args.kafka_group

    # Start HTTP server for health checks + metrics
    port = settings.index_metrics_port
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    http_thread = threading.Thread(target=server.serve_forever, daemon=True)
    http_thread.start()
    print(f"Health server listening on :{port} (/health /ready /metrics)")

    # Start Kafka consumer loop (runs until SIGTERM)
    consumer = IndexConsumer()
    consumer.run()

    server.shutdown()


if __name__ == "__main__":
    main()
