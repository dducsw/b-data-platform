@echo off
REM Clean up script for Big Data Platform

echo 🧹 Big Data Platform - Cleanup Script
echo ======================================
echo.

echo ⚠️  WARNING: This will stop all services and remove all data!
echo ⚠️  Nhấn CTRL+C để hủy bỏ, hoặc nhấn Enter để tiếp tục...
pause >nul

echo 📦 Stopping all services...
docker-compose down

echo 🗑️  Removing all volumes and data...
docker-compose down -v

echo 🧽 Cleaning Docker system...
docker system prune -f

echo 📊 Docker storage after cleanup:
docker system df

echo.
echo ✅ Platform cleanup completed!
echo.
echo 📋 To restart the platform:
echo   - Run: .\scripts\start-platform.bat
echo   - Or: docker-compose up -d
echo.
pause