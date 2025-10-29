#!/bin/bash

# Start Data Platform
echo "Starting Data Platform..."

# Start core services first
echo "Starting core services (MinIO)..."
docker-compose up -d minio

echo "Waiting for core services to be ready..."
sleep 30

# Start Kafka cluster
echo "Starting Kafka cluster..."
docker-compose up -d kafka-controller kafka-broker-1 kafka-broker-2

echo "Waiting for Kafka cluster to be ready..."
sleep 30

# Start Spark cluster
echo "Starting Spark cluster..."
docker-compose up -d spark-master spark-worker-1 spark-worker-2

echo "Waiting for Spark cluster to be ready..."
sleep 20

echo "Starting Gravitino and Iceberg REST Catalog..."
docker-compose up -d gravitino iceberg-rest

echo "Data Platform started successfully!"
echo ""
echo "Access URLs:"
echo "- Spark Master UI: http://localhost:8080"
echo "- Spark Worker 1 UI: http://localhost:8081"
echo "- Spark Worker 2 UI: http://localhost:8082"
echo "- MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)"
echo "- Apache Gravitino: http://localhost:8090"
echo "- Iceberg REST Catalog: http://localhost:8181"
echo "- Kafka Brokers: localhost:9092, localhost:9094"