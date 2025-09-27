#!/bin/bash

# Start Data Platform
echo "Starting Data Platform..."

# Start core services first
echo "Starting core services (PostgreSQL, MinIO)..."
docker-compose up -d postgres minio

echo "Waiting for core services to be ready..."
sleep 30

# Start Kafka cluster
echo "Starting Kafka cluster..."
docker-compose up -d kafka-controller kafka-broker-1 kafka-broker-2

echo "Waiting for Kafka cluster to be ready..."
sleep 30

# Start Hive Metastore
echo "Starting Hive Metastore..."
docker-compose up -d hive-metastore

echo "Waiting for Hive Metastore to be ready..."
sleep 20

# Start Spark cluster
echo "Starting Spark cluster..."
docker-compose up -d spark-master spark-worker-1 spark-worker-2

echo "Waiting for Spark cluster to be ready..."
sleep 20

# Start Kafka consumers
echo "Starting Kafka consumers..."
docker-compose up -d kafka-consumer-1 kafka-consumer-2

echo "Data Platform started successfully!"
echo ""
echo "Access URLs:"
echo "- Spark Master UI: http://localhost:8080"
echo "- Spark Worker 1 UI: http://localhost:8081"
echo "- Spark Worker 2 UI: http://localhost:8082"
echo "- MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)"
echo "- PostgreSQL: localhost:5432 (hive/hive123)"
echo "- Kafka Brokers: localhost:9092, localhost:9094"
echo "- Hive Metastore: localhost:9083"