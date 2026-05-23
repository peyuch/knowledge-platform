#!/bin/bash
echo "=== Verifying knowledge-platform infrastructure ==="

# PostgreSQL
docker compose -f docker/docker-compose.yml exec -T postgres pg_isready -U postgres && echo "✅ PostgreSQL" || echo "❌ PostgreSQL"

# MinIO
curl -sf http://localhost:9000/minio/health/live > /dev/null && echo "✅ MinIO" || echo "❌ MinIO"

# RabbitMQ
docker compose -f docker/docker-compose.yml exec -T rabbitmq rabbitmq-diagnostics -q ping > /dev/null && echo "✅ RabbitMQ" || echo "❌ RabbitMQ"

# Redis
docker compose -f docker/docker-compose.yml exec -T redis redis-cli ping > /dev/null && echo "✅ Redis" || echo "❌ Redis"

# Elasticsearch
curl -sf http://localhost:9200/_cluster/health | grep -q '"status":"green\|yellow"' && echo "✅ Elasticsearch" || echo "❌ Elasticsearch"

# Neo4j
curl -sf http://localhost:7474 > /dev/null && echo "✅ Neo4j" || echo "❌ Neo4j"

# Kafka
docker compose -f docker/docker-compose.yml exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1 && echo "✅ Kafka" || echo "❌ Kafka"

echo "=== Verification complete ==="
