#!/bin/bash

echo "Checking platform services..."
echo "================================="

echo "Docker container status:"
docker-compose ps --format "table {{.Names}}\t{{.State}}\t{{.Ports}}"
echo

echo "Checking service endpoints:"
SERVICES=(
    "9001:MinIO Console"
    "8080:Spark Master UI"
    "8081:Spark Worker 1 UI"
    "8082:Spark Worker 2 UI"
    "8090:Apache Gravitino"
    "8181:Iceberg REST Catalog"
)

for service in "${SERVICES[@]}"; do
    PORT="${service%%:*}"
    NAME="${service#*:}"
    if curl -s --connect-timeout 2 "http://localhost:$PORT" > /dev/null 2>&1; then
        echo "  [READY] $NAME at http://localhost:$PORT"
    else
        echo "  [NOT READY] $NAME at http://localhost:$PORT"
    fi
done

echo
echo "Checking Kafka broker ports:"
KAFKA_PORTS=(
    "9092:Kafka Broker 1"
    "9094:Kafka Broker 2"
)

for kafka_port in "${KAFKA_PORTS[@]}"; do
    PORT="${kafka_port%%:*}"
    NAME="${kafka_port#*:}"
    if ss -lnt | grep -q ":$PORT"; then
        echo "  [LISTENING] $NAME on port $PORT"
    else
        echo "  [NOT LISTENING] $NAME on port $PORT"
    fi
done

echo
echo "Resource Usage:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"

echo
echo "Health check complete."