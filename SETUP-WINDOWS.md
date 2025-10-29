# 🚀 Big Data Platform - Windows Setup Guide

## ✅ Prerequisites Check

Trước khi bắt đầu, đảm bảo hệ thống của bạn đáp ứng:

### Yêu cầu hệ thống:
- **OS**: Windows 10/11 Pro/Enterprise (với Hyper-V)
- **RAM**: Tối thiểu 8GB (khuyến nghị 16GB+)
- **Disk**: 20GB dung lượng trống
- **CPU**: 4 cores trở lên

### Phần mềm cần thiết:
- ✅ **Docker Desktop for Windows** (version 4.0+)
- ✅ **Git for Windows**
- ✅ **PowerShell** (đã có sẵn)

## 🔧 Step-by-Step Setup

### 1. Chuẩn bị Docker
```powershell
# Kiểm tra Docker
docker --version
docker-compose --version

# Cấu hình Docker (khuyến nghị)
# - Memory: 8GB+
# - CPU: 4+ cores
# - Enable Kubernetes (optional)
```

### 2. Clone và Setup Project
```powershell
# Clone repository
git clone <your-repo-url>
cd b-data-platform

# Kiểm tra compatibility
.\scripts\version-check.bat
```

### 3. Build Images
```powershell
# Pull base images
docker-compose pull

# Build custom Spark images (mất ~10-15 phút)
docker-compose build

# Kiểm tra images
docker images | findstr "spark\|kafka\|minio\|gravitino"
```

### 4. Start Platform
```powershell
# Khởi động tất cả services
.\scripts\start-platform.bat

# Hoặc manual:
docker-compose up -d

# Kiểm tra status
docker-compose ps
```

### 5. Verify Setup
```powershell
# Health check
.\scripts\health-check.bat

# Advanced testing
.\scripts\advanced-test.bat
```

## 🌐 Access URLs

Sau khi platform khởi động thành công:

| Service | URL | Credentials |
|---------|-----|-------------|
| **Spark Master UI** | http://localhost:8080 | - |
| **Spark Worker 1** | http://localhost:8081 | - |
| **Spark Worker 2** | http://localhost:8082 | - |
| **MinIO Console** | http://localhost:9001 | `minioadmin/minioadmin123` |
| **Apache Gravitino** | http://localhost:8090 | - |
| **Iceberg REST** | http://localhost:8181 | - |
| **Kafka Brokers** | localhost:9092, localhost:9094 | - |

## 🧪 Quick Tests

### Test MinIO
```powershell
# Kiểm tra bucket
docker-compose exec minio-init mc ls myminio/
```

### Test Kafka
```powershell
# List topics
docker-compose exec kafka-broker-1 kafka-topics.sh --bootstrap-server localhost:29092 --list

# Create test topic
docker-compose exec kafka-broker-1 kafka-topics.sh --bootstrap-server localhost:29092 --create --topic test --partitions 3 --replication-factor 2
```

### Test Spark + Iceberg
```powershell
# Run example
docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/scripts/iceberg-example.py
```

## 🚨 Common Issues & Solutions

### Issue 1: Build Fails
**Giải pháp:**
```powershell
# Clear cache và rebuild
docker-compose build --no-cache

# Hoặc pull individual images
docker pull apache/spark:3.5.5
docker pull confluentinc/cp-kafka:7.7.0
```

### Issue 2: Port Conflicts
**Kiểm tra:**
```powershell
netstat -an | findstr ":8080\|:9092"
```
**Giải pháp:** Tắt các ứng dụng khác hoặc thay đổi ports trong `docker-compose.yml`

### Issue 3: Memory Issues
**Kiểm tra:**
```powershell
docker system info | findstr "Total Memory"
```
**Giải pháp:** Tăng memory allocation cho Docker Desktop (Settings > Resources)

### Issue 4: Slow Startup
**Bình thường:** Services cần 2-3 phút để khởi động hoàn toàn
**Kiểm tra:** `docker-compose logs [service-name]`

## 🔧 Maintenance Commands

```powershell
# Stop platform
.\scripts\stop-platform.bat

# View logs
docker-compose logs -f [service-name]

# Restart service
docker-compose restart [service-name]

# Complete cleanup
.\scripts\cleanup.bat

# Troubleshooting
.\scripts\troubleshoot.bat
```

## 📊 Performance Monitoring

```powershell
# Real-time stats
docker stats

# Resource usage
docker system df

# Service health
.\scripts\health-check.bat
```

## 🎯 Next Steps

1. **Explore Examples**: Check `/scripts/` folder for sample applications
2. **Custom Development**: Add your own Spark jobs to `/scripts/`
3. **Production Setup**: Review `docker-compose.prod.yml` for production deployment
4. **Monitoring**: Set up Grafana/Prometheus for advanced monitoring

## 📞 Support

- **Logs**: `docker-compose logs [service-name]`
- **Debug**: `docker-compose exec [service-name] bash`
- **Troubleshoot**: `.\scripts\troubleshoot.bat`

---
**Happy Data Engineering! 🚀**