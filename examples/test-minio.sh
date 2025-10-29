#!/bin/bash

echo "🚀 Starting Spark MinIO Direct Write Test"
echo "========================================"

# Start the platform
echo "🔧 Starting data platform..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 30

# Check MinIO is ready
echo "🧪 Testing MinIO connection..."
docker exec -it minio-init mc ls myminio/

# Run the MinIO example script
echo "🎯 Running MinIO direct write example..."
docker exec -it spark-master python /opt/spark/scripts/minio-example.py

echo "✅ Test completed!"
echo "🌐 Access MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)"
echo "🌐 Access Spark UI: http://localhost:8080"