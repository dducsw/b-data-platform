# Big Data Platform

A comprehensive data platform built with Docker Compose, featuring Apache Spark 4.0, Apache Kafka 3.9 with KRaft, Apache Iceberg, Apache Gravitino, and MinIO.

## 🏗️ Architecture

This platform provides a complete big data ecosystem with the following components:

### Core Components

- **Apache Spark 3.5**: Distributed computing engine (1 Master + 2 Workers)
- **Apache Kafka 3.9**: Streaming platform with KRaft mode (2 Brokers + 1 Controller)
- **Apache Iceberg 1.9.2**: Open table format with ACID transactions and time travel
- **Apache Gravitino**: Universal metadata management for multi-catalog data
- **Iceberg REST Catalog 1.6.0**: RESTful catalog service for Iceberg tables
- **MinIO**: S3-compatible object storage

### Key Features

- ✅ **ACID Transactions**: Iceberg provides ACID guarantees with snapshot isolation
- ✅ **Time Travel**: Query historical versions of data with Iceberg snapshots
- ✅ **Schema Evolution**: Safe schema changes without breaking existing queries
- ✅ **Table Format V2**: Latest Iceberg format with merge-on-read and improved performance
- ✅ **Universal Metadata**: Gravitino manages metadata across multiple data catalogs
- ✅ **Stream Processing**:Data processing with Spark Streaming + Kafka
- ✅ **Object Storage**: S3-compatible storage with MinIO
- ✅ **Scalable**: Distributed processing with Spark cluster
- ✅ **CI/CD**: GitHub Actions for automated testing and deployment

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Git
- At least 8GB RAM (16GB recommended)
- 20GB free disk space

### 1. Clone the Repository

```bash
git clone <repository-url>
cd b-data-platform
```

### 2. Start the Platform

```bash
# Start all services
docker-compose up -d

# Or use the helper script (Linux/Mac)
chmod +x scripts/*.sh
./scripts/start-platform.sh
```

### 3. Access the Services

Once all services are running, you can access:

| Service | URL | Credentials |
|---------|-----|-------------|
| Spark Master UI | http://localhost:8080 | - |
| Spark Worker 1 UI | http://localhost:8081 | - |
| Spark Worker 2 UI | http://localhost:8082 | - |
| MinIO Console | http://localhost:9002 | minioadmin/minioadmin123 |
| Apache Gravitino | http://localhost:8090 | - |
| Gravitino Iceberg REST | http://localhost:9001 | - |
| Iceberg REST Catalog | http://localhost:8181 | - |
| Kafka Brokers | localhost:9092, localhost:9094 | - |
| Kafka Controller | localhost:9093 | - |

## 📊 Usage Examples

### Apache Iceberg Example

Run the included Iceberg example:

```bash
docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/scripts/iceberg-example.py
```


### Creating Kafka Topics

```bash
# Create a topic
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

### MinIO Operations

Access MinIO console at http://localhost:9002 with credentials `minioadmin/minioadmin123`

Or use mc client:
```bash
docker run --rm --network b-data-platform_data-platform \
    minio/mc:latest mc ls myminio/
```

### Gravitino and Iceberg Catalogs

Gravitino provides universal metadata management and also exposes an Iceberg REST service on port 9001:
- Gravitino UI: http://localhost:8090
- Gravitino Iceberg REST: http://localhost:9001

The dedicated Iceberg REST Catalog is available at:
- Iceberg REST API: http://localhost:8181

## 🧪 Testing


### Manual Health Checks

```bash
# Check all services
docker-compose ps

# Check individual service logs
docker-compose logs spark-master
docker-compose logs kafka-broker-1
docker-compose logs minio
```

## 🔄 CI/CD

The platform includes GitHub Actions workflows for:

- **Continuous Integration**: Builds and tests all services
- **Security Scanning**: Trivy vulnerability scanning
- **Container Registry**: Pushes images to GitHub Container Registry

## 📁 Project Structure

```
b-data-platform/
├── .github/                  # GitHub Actions CI/CD workflows
├── config/                   # Configuration files
│   ├── gravitino/           # Apache Gravitino configuration
│   └── spark/               # Spark configuration files
├── data/                     # Shared data directory (mounted to containers)
├── docker/                   # Custom Docker images
│   └── spark/               # Custom Spark image with Iceberg support
├── src/                      # Source code and examples
│   ├── examples/            # Example scripts and utilities
│   │   ├── gravitino-example.py
│   │   ├── health-check.bat # Windows health check script
│   │   ├── health-check.sh  # Linux health check script
│   │   └── iceberg-example.py
│   └── spark/               # Spark application source code
├── .env                      # Environment variables
├── .gitignore               # Git ignore file
├── docker-compose.yml       # Main Docker Compose configuration
├── docker-compose.prod.yml  # Production Docker Compose configuration
├── Makefile                 # Build and management commands
├── README.md                # This file
└── SETUP-WINDOWS.md         # Windows setup guide
```

## 🛠️ Development

### Custom Spark Applications

Place your applications in the `scripts/` directory and submit them:

```bash
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark/scripts/your-app.py
```

## 🚨 Troubleshooting

### Common Issues

1. **Out of Memory**: Increase Docker memory to 8GB+
2. **Port Conflicts**: Change ports in docker-compose.yml
3. **Slow Startup**: Wait 2-3 minutes for all services
4. **Permission Issues**: Run `chmod +x scripts/*.sh`

### Cleanup

```bash
# Stop and remove everything
docker-compose down -v

# Clean up system resources
docker system prune -f
```