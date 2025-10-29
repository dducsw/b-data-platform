@echo off
setlocal enabledelayedexpansion

echo Checking platform services...
echo =================================

echo Docker container status:
docker-compose ps --format "table {{.Names}}\t{{.State}}\t{{.Ports}}"
echo.

echo Checking service endpoints:
for %%s in (
    "9001:MinIO Console"
    "8080:Spark Master UI"
    "8081:Spark Worker 1 UI"
    "8082:Spark Worker 2 UI"
    "8090:Apache Gravitino"
    "8181:Iceberg REST Catalog"
) do (
    for /f "tokens=1,2 delims=:" %%a in ("%%s") do (
        curl -s --connect-timeout 2 "http://localhost:%%a" > nul 2>&1
        if !errorlevel! == 0 (
            echo   [READY] %%b at http://localhost:%%a
        ) else (
            echo   [NOT READY] %%b at http://localhost:%%a
        )
    )
)

echo.
echo Checking Kafka broker ports:
for %%p in (
    "9092:Kafka Broker 1"
    "9094:Kafka Broker 2"
) do (
    for /f "tokens=1,2 delims=:" %%a in ("%%p") do (
        netstat -an | findstr ":%%a" > nul 2>&1
        if !errorlevel! == 0 (
            echo   [LISTENING] %%b on port %%a
        ) else (
            echo   [NOT LISTENING] %%b on port %%a
        )
    )
)

echo.
echo Resource Usage:
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"

echo.
echo Health check complete.
endlocal