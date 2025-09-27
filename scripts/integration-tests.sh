#!/bin/bash

# Data Platform Integration Tests

set -e

echo "Running integration tests for Data Platform..."

# Test 1: PostgreSQL Health
echo "Testing PostgreSQL connection..."
docker-compose exec -T postgres pg_isready -U hive -d hive_metastore
if [ $? -eq 0 ]; then
    echo "✅ PostgreSQL is healthy"
else
    echo "❌ PostgreSQL test failed"
    exit 1
fi

# Test 2: MinIO Health
echo "Testing MinIO connection..."
if curl -f http://localhost:9001/minio/health/live; then
    echo "✅ MinIO is healthy"
else
    echo "❌ MinIO test failed"
    exit 1
fi

# Test 3: Kafka Health
echo "Testing Kafka brokers..."
docker-compose exec -T kafka-broker-1 kafka-topics.sh --bootstrap-server kafka-broker-1:29092 --list
if [ $? -eq 0 ]; then
    echo "✅ Kafka broker 1 is healthy"
else
    echo "❌ Kafka broker 1 test failed"
    exit 1
fi

docker-compose exec -T kafka-broker-2 kafka-topics.sh --bootstrap-server kafka-broker-2:29094 --list
if [ $? -eq 0 ]; then
    echo "✅ Kafka broker 2 is healthy"
else
    echo "❌ Kafka broker 2 test failed"
    exit 1
fi

# Test 4: Spark Master Health
echo "Testing Spark Master..."
if curl -f http://localhost:8080; then
    echo "✅ Spark Master is healthy"
else
    echo "❌ Spark Master test failed"
    exit 1
fi

# Test 5: Spark Workers Health
echo "Testing Spark Workers..."
if curl -f http://localhost:8081; then
    echo "✅ Spark Worker 1 is healthy"
else
    echo "❌ Spark Worker 1 test failed"
    exit 1
fi

if curl -f http://localhost:8082; then
    echo "✅ Spark Worker 2 is healthy"
else
    echo "❌ Spark Worker 2 test failed"
    exit 1
fi

# Test 6: Hive Metastore Health
echo "Testing Hive Metastore..."
if nc -z localhost 9083; then
    echo "✅ Hive Metastore is healthy"
else
    echo "❌ Hive Metastore test failed"
    exit 1
fi

# Test 7: Create test topic and produce/consume messages
echo "Testing Kafka produce/consume..."
docker-compose exec -T kafka-broker-1 kafka-topics.sh \
    --bootstrap-server kafka-broker-1:29092 \
    --create --if-not-exists \
    --topic integration-test \
    --partitions 2 \
    --replication-factor 2

echo "test-message-$(date)" | docker-compose exec -T kafka-broker-1 kafka-console-producer.sh \
    --bootstrap-server kafka-broker-1:29092 \
    --topic integration-test

sleep 2

CONSUMED_MESSAGE=$(timeout 10s docker-compose exec -T kafka-broker-1 kafka-console-consumer.sh \
    --bootstrap-server kafka-broker-1:29092 \
    --topic integration-test \
    --from-beginning \
    --max-messages 1 2>/dev/null || true)

if [[ "$CONSUMED_MESSAGE" == *"test-message"* ]]; then
    echo "✅ Kafka produce/consume test passed"
else
    echo "❌ Kafka produce/consume test failed"
    exit 1
fi

# Test 8: Test MinIO bucket operations
echo "Testing MinIO bucket operations..."
docker run --rm --network b-data-platform_data-platform \
    minio/mc:latest sh -c "
    mc alias set minio http://minio:9000 minioadmin minioadmin123 &&
    mc mb minio/test-bucket --ignore-existing &&
    echo 'test data' | mc pipe minio/test-bucket/test-file.txt &&
    mc cat minio/test-bucket/test-file.txt
"

if [ $? -eq 0 ]; then
    echo "✅ MinIO operations test passed"
else
    echo "❌ MinIO operations test failed"
    exit 1
fi

echo "🎉 All integration tests passed!"