#!/bin/bash

# Health check script for the data platform services
echo "=== Data Platform Health Check ==="
echo ""

# Check PostgreSQL
echo "Checking PostgreSQL..."
if curl -s --connect-timeout 5 http://localhost:5432 > /dev/null 2>&1; then
    echo "✅ PostgreSQL is accessible on port 5432"
else
    echo "❌ PostgreSQL is not accessible"
fi

# Check Kafka UI
echo "Checking Kafka UI..."
if curl -s --connect-timeout 5 http://localhost:8888 > /dev/null 2>&1; then
    echo "✅ Kafka UI is accessible on port 8888"
else
    echo "❌ Kafka UI is not accessible"
fi

# Check Gravitino
echo "Checking Gravitino..."
if curl -s --connect-timeout 5 http://localhost:8090 > /dev/null 2>&1; then
    echo "✅ Gravitino is accessible on port 8090"
else
    echo "❌ Gravitino is not accessible"
fi

# Check Gravitino Iceberg REST
echo "Checking Gravitino Iceberg REST..."
if curl -s --connect-timeout 5 http://localhost:9001 > /dev/null 2>&1; then
    echo "✅ Gravitino Iceberg REST is accessible on port 9001"
else
    echo "❌ Gravitino Iceberg REST is not accessible"
fi

# Check MinIO
echo "Checking MinIO..."
if curl -s --connect-timeout 5 http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo "✅ MinIO is accessible on port 9000"
else
    echo "❌ MinIO is not accessible"
fi

# Check Kafka Brokers
echo "Checking Kafka Brokers..."
if nc -z localhost 9092 2>/dev/null; then
    echo "✅ Kafka Broker 1 is accessible on port 9092"
else
    echo "❌ Kafka Broker 1 is not accessible"
fi

if nc -z localhost 9094 2>/dev/null; then
    echo "✅ Kafka Broker 2 is accessible on port 9094"
else
    echo "❌ Kafka Broker 2 is not accessible"
fi

echo ""
echo "=== Service URLs ==="
echo "Kafka UI: http://localhost:8888"
echo "Gravitino UI: http://localhost:8090"
echo "Gravitino Iceberg REST: http://localhost:9001"
echo "MinIO Console: http://localhost:9002"
echo "Spark Master UI: http://localhost:8080"
echo "PostgreSQL: localhost:5432 (user: gravitino, password: gravitino123, db: gravitino)"