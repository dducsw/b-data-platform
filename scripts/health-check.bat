@echo off
REM Script kiểm tra tính sẵn sàng của tất cả services

echo 🔍 Kiểm tra tình trạng platform services...
echo ==========================================
echo.

echo 📦 Kiểm tra Docker containers...
docker-compose ps

echo.
echo 🌐 Kiểm tra kết nối các services...

echo ⏳ Kiểm tra MinIO (Object Storage)...
curl -s http://localhost:9001 > nul 2>&1
if %errorlevel% == 0 (
    echo ✅ MinIO Console: http://localhost:9001 - READY
) else (
    echo ❌ MinIO Console: http://localhost:9001 - NOT READY
)

echo ⏳ Kiểm tra Spark Master...
curl -s http://localhost:8080 > nul 2>&1
if %errorlevel% == 0 (
    echo ✅ Spark Master UI: http://localhost:8080 - READY
) else (
    echo ❌ Spark Master UI: http://localhost:8080 - NOT READY
)

echo ⏳ Kiểm tra Spark Worker 1...
curl -s http://localhost:8081 > nul 2>&1
if %errorlevel% == 0 (
    echo ✅ Spark Worker 1 UI: http://localhost:8081 - READY
) else (
    echo ❌ Spark Worker 1 UI: http://localhost:8081 - NOT READY
)

echo ⏳ Kiểm tra Spark Worker 2...
curl -s http://localhost:8082 > nul 2>&1
if %errorlevel% == 0 (
    echo ✅ Spark Worker 2 UI: http://localhost:8082 - READY
) else (
    echo ❌ Spark Worker 2 UI: http://localhost:8082 - NOT READY
)

echo ⏳ Kiểm tra Apache Gravitino...
curl -s http://localhost:8090 > nul 2>&1
if %errorlevel% == 0 (
    echo ✅ Apache Gravitino: http://localhost:8090 - READY
) else (
    echo ❌ Apache Gravitino: http://localhost:8090 - NOT READY
)

echo ⏳ Kiểm tra Iceberg REST Catalog...
curl -s http://localhost:8181 > nul 2>&1
if %errorlevel% == 0 (
    echo ✅ Iceberg REST Catalog: http://localhost:8181 - READY
) else (
    echo ❌ Iceberg REST Catalog: http://localhost:8181 - NOT READY
)

echo.
echo 🔌 Kiểm tra Kafka brokers...
timeout /t 2 /nobreak > nul 2>&1
netstat -an | findstr ":9092" > nul 2>&1
if %errorlevel% == 0 (
    echo ✅ Kafka Broker 1: localhost:9092 - LISTENING
) else (
    echo ❌ Kafka Broker 1: localhost:9092 - NOT LISTENING
)

netstat -an | findstr ":9094" > nul 2>&1
if %errorlevel% == 0 (
    echo ✅ Kafka Broker 2: localhost:9094 - LISTENING
) else (
    echo ❌ Kafka Broker 2: localhost:9094 - NOT LISTENING
)

echo.
echo 📊 Resource Usage:
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"

echo.
echo 📋 Health Summary:
echo ==================
docker-compose ps --format table
echo.
echo ✅ Kiểm tra hoàn tất!