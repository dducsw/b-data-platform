# Big Data Platform - Windows Setup Guide

## Prerequisites

### System Requirements:
- Windows 10/11 with Docker Desktop
- 8GB RAM (16GB recommended)
- 20GB free disk space

### Required Software:
- Docker Desktop for Windows
- Git for Windows
- PowerShell

## Setup Steps

### 1. Check Docker
```powershell
docker --version
docker-compose --version
```

### 2. Clone Repository
```powershell
git clone <your-repo-url>
cd b-data-platform
```

### 3. Start Platform
```powershell
docker-compose up -d
```

### 4. Verify Setup
```powershell
docker-compose ps
```

## Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Spark Master UI | http://localhost:8080 | - |
| Spark Worker 1 | http://localhost:8081 | - |
| Spark Worker 2 | http://localhost:8082 | - |
| MinIO Console | http://localhost:9002 | minioadmin/minioadmin123 |
| Apache Gravitino | http://localhost:8090 | - |
| Iceberg REST Catalog | http://localhost:8181 | - |
| Kafka Brokers | localhost:9092, localhost:9094 | - |

## Quick Tests

### Test MinIO
```powershell
docker run --rm --network b-data-platform_data-platform minio/mc:latest mc ls myminio/
```

### Test Kafka
```powershell
docker-compose exec kafka-broker-1 kafka-topics.sh --bootstrap-server kafka-broker-1:29092 --list
```

### Test Spark
```powershell
docker-compose exec spark-master spark-shell --master spark://spark-master:7077
```

## Common Commands

```powershell
# Stop all services
docker-compose down

# View logs
docker-compose logs -f

# Clean up (removes all data)
docker-compose down -v

# Restart services
docker-compose restart
```

## Troubleshooting

- **Port conflicts**: Change ports in docker-compose.yml
- **Memory issues**: Increase Docker Desktop memory allocation
- **Slow startup**: Wait 2-3 minutes for all services to start