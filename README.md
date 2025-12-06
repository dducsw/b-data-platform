
# Big Data Platform

An integrated big data platform with modern components, deployed via Docker Compose. Suitable for streaming, batch processing, analytics, and metadata management.

## 🏗️ System Architecture

Main components:

- **Apache Spark 3.5**: Distributed computing engine (1 Master, 2 Workers)
- **Apache Kafka 3.9 (KRaft)**: Streaming platform (2 Brokers, 1 Controller)
- **Apache Iceberg 1.9.2**: ACID-compliant table format with time travel and schema evolution
- **Iceberg REST Catalog**: Metadata management for Iceberg via REST API
- **Apache Gravitino**: Universal metadata management, multi-catalog support
- **MinIO**: S3-compatible object storage
- **Doris**: OLAP database for big data analytics
- **MySQL**: Metadata or application backend database
- **Schema Registry**: Kafka schema management
- **Kafka UI**: Kafka topic/message management interface
- **Grafana**: Monitoring and data visualization

## ⚡ Quick Start

### System Requirements
- Docker & Docker Compose
- Git
- Minimum 8GB RAM (16GB recommended)
- 20GB free disk space

### 1. Clone the repository
```bash
git clone <repository-url>
cd b-data-platform
```

### 2. Start all services
```bash
docker-compose up -d
```

### 3. Access services
| Service              | URL                        | Credentials                |
|----------------------|----------------------------|----------------------------|
| Spark Master UI      | http://localhost:8080      | -                          |
| Spark Worker 1 UI    | http://localhost:8081      | -                          |
| Spark Worker 2 UI    | http://localhost:8082      | -                          |
| MinIO Console        | http://localhost:9002      | minioadmin/minioadmin123   |
| Gravitino UI         | http://localhost:8090      | -                          |
| Iceberg REST Catalog | http://localhost:8181      | -                          |
| Doris FE             | http://localhost:8030      | -                          |
| Doris BE             | http://localhost:8040      | -                          |
| Kafka UI             | http://localhost:8888      | -                          |
| Schema Registry      | http://localhost:8088      | -                          |
| Grafana              | http://localhost:3000      | admin/admin                |

## 📊 Usage Examples

### Run Spark submit with Iceberg
```bash
docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/scripts/iceberg-example.py
```

### Create and manage Kafka topics
```bash
# Create topic
docker-compose exec kafka-broker-1 kafka-topics.sh \
    --bootstrap-server kafka-broker-1:29092 \
    --create \
    --topic my-topic \
    --partitions 3 \
    --replication-factor 2

# List topics
docker-compose exec kafka-broker-1 kafka-topics.sh \
    --bootstrap-server kafka-broker-1:29092 \
    --list
```

### Access MinIO
Go to http://localhost:9002 with `minioadmin/minioadmin123`.
Or use mc client:
```bash
docker run --rm --network b-data-platform_data-platform minio/mc:latest mc ls myminio/
```

### Access Doris
FE: http://localhost:8030  
BE: http://localhost:8040

### Access Kafka UI
http://localhost:8888

### Access Grafana
http://localhost:3000 (admin/admin)

## 🧪 Monitoring & Health Check
```bash
# Check service status
docker-compose ps
# View service logs
docker-compose logs spark-master
docker-compose logs kafka-broker-1
docker-compose logs minio
```

## 🔄 CI/CD
Integrated GitHub Actions:
- Automated build & test
- Security scan (Trivy)
- Push images to GitHub Container Registry

## 📁 Folder Structure
```
b-data-platform/
├── .github/                  # CI/CD workflows
├── config/                   # Service configurations
│   ├── doris/                # Doris FE/BE config
│   ├── rest/                 # Iceberg REST Catalog config
│   └── spark/                # Spark config
├── data/                     # Shared data
├── docker/                   # Dockerfiles for services
│   ├── rest/
│   └── spark/
├── docs/                     # Internal documentation
├── scripts/                  # Spark apps, examples, ETL
│   ├── bronze/
│   ├── example/
│   ├── gold/
│   └── silver/
├── .env                      # Environment variables
├── .gitignore                # Git ignore
├── docker-compose.yml        # Docker Compose config
├── docker-compose.prod.yml   # Docker Compose for production
├── Makefile                  # Build/management commands
├── README.md                 # This file
└── SETUP-WINDOWS.md          # Windows setup guide
```

## 🛠️ Development
Place your Spark applications in the `scripts/` folder and submit jobs:
```bash
docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/scripts/your-app.py
```

## 🚨 Troubleshooting
### Common Issues
1. **Out of RAM**: Increase Docker RAM to 8GB+
2. **Port conflict**: Change ports in `docker-compose.yml`
3. **Slow startup**: Wait 2-3 minutes for all services
4. **File permission errors**: Run `chmod +x scripts/*.sh`

### Cleanup
```bash
# Stop and remove all services
docker-compose down -v
# System cleanup
docker system prune -f
```