@echo off
echo 🚀 Starting Spark MinIO Direct Write Test
echo ========================================

REM Start the platform
echo 🔧 Starting data platform...
docker-compose up -d

REM Wait for services to be ready
echo ⏳ Waiting for services to start...
timeout /t 30 /nobreak >nul

REM Check MinIO is ready
echo 🧪 Testing MinIO connection...
docker exec minio-init mc ls myminio/

REM Run the MinIO example script
echo 🎯 Running MinIO direct write example...
docker exec spark-master python /opt/spark/scripts/minio-example.py

echo ✅ Test completed!
echo 🌐 Access MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)
echo 🌐 Access Spark UI: http://localhost:8080
pause