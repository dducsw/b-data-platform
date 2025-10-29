@echo off
REM Advanced connectivity test for Big Data Platform

echo 🧪 Big Data Platform - Advanced Connectivity Test
echo ==================================================
echo.

echo 📋 Phase 1: Basic Service Health Check
echo =======================================

echo ⏳ Checking MinIO API endpoint...
curl -X GET http://localhost:9000/minio/health/live -w "Status: %%{http_code}\n" -s -o nul

echo ⏳ Checking Kafka broker connectivity...
timeout /t 1 /nobreak > nul
telnet localhost 9092 2>nul | echo Kafka Broker 1: Connection test

echo ⏳ Checking Spark Master API...
curl -X GET http://localhost:8080/api/v1/applications -w "Status: %%{http_code}\n" -s -o nul

echo.
echo 📋 Phase 2: Integration Tests
echo =============================

echo ⏳ Testing Kafka topic creation...
docker-compose exec -T kafka-broker-1 kafka-topics.sh --bootstrap-server localhost:29092 --list > nul 2>&1
if %errorlevel% == 0 (
    echo ✅ Kafka cluster operational
) else (
    echo ❌ Kafka cluster issues detected
)

echo ⏳ Testing MinIO bucket access...
docker-compose exec -T minio-init mc ls myminio/ > nul 2>&1
if %errorlevel% == 0 (
    echo ✅ MinIO buckets accessible
) else (
    echo ❌ MinIO bucket access issues
)

echo ⏳ Testing Spark cluster status...
curl -s http://localhost:8080/api/v1/applications | findstr "[]" > nul 2>&1
if %errorlevel% == 0 (
    echo ✅ Spark cluster API responsive
) else (
    echo ❌ Spark cluster API issues
)

echo.
echo 📋 Phase 3: Port Accessibility
echo ==============================

set ports=8080 8081 8082 8090 8181 9000 9001 9092 9094
for %%p in (%ports%) do (
    netstat -an | findstr ":%%p" > nul 2>&1
    if !errorlevel! == 0 (
        echo ✅ Port %%p - OPEN
    ) else (
        echo ❌ Port %%p - CLOSED
    )
)

echo.
echo 📋 Phase 4: Docker Network Connectivity
echo =======================================

echo ⏳ Testing internal network connectivity...
docker network ls | findstr data-platform > nul 2>&1
if %errorlevel% == 0 (
    echo ✅ Docker network 'data-platform' exists
) else (
    echo ❌ Docker network 'data-platform' missing
)

echo.
echo 📋 Phase 5: Compatibility Verification
echo ======================================

echo ⏳ Checking Spark-Iceberg compatibility...
docker-compose exec -T spark-master ls /opt/spark/jars/ | findstr iceberg > nul 2>&1
if %errorlevel% == 0 (
    echo ✅ Iceberg jars present in Spark
) else (
    echo ❌ Iceberg jars missing in Spark
)

echo.
echo 🎯 Test Summary:
echo ===============
echo - Service Health: Use 'docker-compose ps' for details
echo - Web UIs: Access via browser using URLs above
echo - Logs: Use 'docker-compose logs [service-name]' for troubleshooting
echo.
echo ✅ Advanced connectivity test completed!
pause