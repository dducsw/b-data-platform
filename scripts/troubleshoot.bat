@echo off
REM Troubleshooting script for Big Data Platform

echo 🔧 Big Data Platform - Troubleshooting Guide
echo =============================================
echo.

echo 📋 Phase 1: Common Issues Check
echo ===============================

echo ⏳ Checking Docker Desktop status...
docker version > nul 2>&1
if %errorlevel% == 0 (
    echo ✅ Docker is running
) else (
    echo ❌ Docker is not running - Start Docker Desktop first!
    pause
    exit /b 1
)

echo ⏳ Checking Docker memory allocation...
docker system info | findstr "Total Memory" > temp_memory.txt
for /f "tokens=3" %%a in (temp_memory.txt) do set memory=%%a
del temp_memory.txt
echo - Current allocation: %memory%
echo - Recommended: 8GB+ (16GB for best performance)

echo ⏳ Checking port conflicts...
netstat -an | findstr ":8080 :8081 :8082 :8090 :8181 :9000 :9001 :9092 :9094" > temp_ports.txt
if exist temp_ports.txt (
    echo ⚠️  Some ports may be in use:
    type temp_ports.txt
    del temp_ports.txt
) else (
    echo ✅ All ports are available
)

echo.
echo 📋 Phase 2: Service-specific Troubleshooting
echo ============================================

echo ⏳ Checking container status...
docker-compose ps --format table

echo.
echo ⏳ Checking container resource usage...
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}"

echo.
echo 📋 Phase 3: Log Analysis
echo ========================

echo ⏳ Recent container logs (last 10 lines):
echo.
echo --- MinIO Logs ---
docker-compose logs --tail=10 minio 2>nul

echo.
echo --- Spark Master Logs ---
docker-compose logs --tail=10 spark-master 2>nul

echo.
echo --- Kafka Broker Logs ---
docker-compose logs --tail=10 kafka-broker-1 2>nul

echo.
echo 📋 Phase 4: Quick Fixes
echo =======================

echo 🔧 Common Solutions:
echo.
echo 1. Service not starting:
echo    - Increase Docker memory to 8GB+
echo    - Run: docker-compose down && docker-compose up -d
echo.
echo 2. Port conflicts:
echo    - Check running applications on ports 8080-9094
echo    - Stop conflicting services or change ports in docker-compose.yml
echo.
echo 3. Build failures:
echo    - Run: docker-compose build --no-cache
echo    - Check internet connection for downloading dependencies
echo.
echo 4. Slow performance:
echo    - Allocate more CPU/Memory to Docker
echo    - Close unnecessary applications
echo.
echo 5. Connectivity issues:
echo    - Run: .\scripts\health-check.bat
echo    - Check Windows Firewall settings
echo.

echo 📞 Advanced Troubleshooting:
echo - Full logs: docker-compose logs [service-name]
echo - Container shell: docker-compose exec [service-name] bash
echo - Restart service: docker-compose restart [service-name]
echo.
echo ✅ Troubleshooting guide completed!
pause