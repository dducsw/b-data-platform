# Big Data Platform

A comprehensive data platform built with Docker Compose, featuring Apache Spark 4.0, Apache Kafka 3.9 with KRaft, Apache Iceberg, Apache Gravitino, and MinIO.

## 🏗️ Architecture

This platform provides a complete big data ecosystem with the following components:

### Core Components

- **Apache Spark 4.0**: Distributed computing engine (1 Master + 2 Workers)
- **Apache Kafka 3.9**: Streaming platform with KRaft mode (2 Brokers + 1 Controller)
- **Apache Iceberg 1.9.2**: Open table format with ACID transactions and time travel (Format Version 2)
- **Apache Gravitino 1.0.0**: Universal metadata management for multi-catalog data
- **Iceberg REST Catalog**: RESTful catalog service for Iceberg tables
- **MinIO**: S3-compatible object storage

### Key Features

- ✅ **ACID Transactions**: Iceberg provides ACID guarantees with snapshot isolation
- ✅ **Time Travel**: Query historical versions of data with Iceberg snapshots
- ✅ **Schema Evolution**: Safe schema changes without breaking existing queries
- ✅ **Table Format V2**: Latest Iceberg format with merge-on-read and improved performance
- ✅ **Universal Metadata**: Gravitino manages metadata across multiple data catalogs
- ✅ **Stream Processing**: Real-time data processing with Spark Streaming + Kafka
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
| MinIO Console | http://localhost:9001 | minioadmin/minioadmin123 |
| Apache Gravitino | http://localhost:8090 | - |
| Iceberg REST Catalog | http://localhost:8181 | - |
| Kafka Brokers | localhost:9092, localhost:9094 | - |

## 📊 Usage Examples

### Apache Iceberg Example

Run the included Iceberg example:

```bash
docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/scripts/iceberg-example.py
```

### Kafka to Iceberg Streaming

Real-time streaming from Kafka to Iceberg tables:

```bash
docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/scripts/kafka-iceberg-streaming.py

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

Access MinIO console at http://localhost:9001 with credentials `minioadmin/minioadmin123`

Or use mc client:
```bash
docker run --rm --network b-data-platform_data-platform \
    minio/mc:latest mc mb minio/datalake --ignore-existing
```

## 🧪 Testing

### Run Integration Tests

```bash
./scripts/integration-tests.sh
```

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
├── .github/workflows/         # GitHub Actions CI/CD
├── config/spark/             # Spark configuration
├── docker/                   # Docker images
│   ├── kafka/               # Kafka consumers
│   └── spark/               # Spark with Delta Lake
├── scripts/                  # Utility scripts and examples
├── data/                     # Shared data directory
├── docker-compose.yml        # Main orchestration
└── README.md
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
- **MinIO**: S3-compatible object storage
- **Delta Lake 4.0**: Open-source storage framework for data lakes

## Quick Start

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd b-data-platform
   ```

2. Start the platform:
   ```bash
   docker-compose up -d
   ```

3. Access services:
   - Spark Master UI: http://localhost:8080
   - MinIO Console: http://localhost:9001 (admin/admin123)
   - Kafka UI: http://localhost:8081

## Services

### Core Services
- **Spark Master**: Port 8080 (UI), 7077 (Spark)
- **Spark Worker**: Port 8081 (UI)
- **Kafka**: Port 9092
- **MinIO**: Port 9000 (API), 9001 (Console)

### Development Tools
- **Jupyter Notebook**: Port 8888
- **Kafka UI**: Port 8082

## Configuration

### Environment Variables
Key environment variables are defined in `.env` file:
- Database credentials
- MinIO access keys
- Service ports

### Volumes
- `./data`: Persistent data storage
- `./config`: Configuration files
- `./notebooks`: Jupyter notebooks

## Usage Examples

### Spark Jobs
```python
from pyspark.sql import SparkSession
from delta import *

spark = SparkSession.builder \
    .appName("DeltaLakeExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Create Delta table
data = spark.range(0, 5)
data.write.format("delta").save("s3a://warehouse/delta-table")
```

### Kafka Producer/Consumer
```python
from kafka import KafkaProducer, KafkaConsumer

# Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer.send('test-topic', b'Hello World')

# Consumer
consumer = KafkaConsumer('test-topic', bootstrap_servers=['localhost:9092'])
for message in consumer:
    print(message.value)
```

## CI/CD

GitHub Actions workflows are configured for:
- Automated testing
- Docker image building
- Security scanning
- Deployment validation

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `docker-compose -f docker-compose.test.yml up --abort-on-container-exit`
5. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) file for details.