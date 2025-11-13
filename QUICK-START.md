# Quick Start Guide - Updated Data Platform

## What's New

✅ **PostgreSQL** - Persistent metadata backend for Gravitino  
✅ **Kafka UI** - Web interface for Kafka management  
✅ **Enhanced Gravitino** - Now uses PostgreSQL for persistent metadata storage  

## Starting the Platform

```bash
# Start all services
docker-compose up -d

# Check logs
docker-compose logs -f gravitino postgres kafka-ui

# Stop all services
docker-compose down
```

## Service URLs

| Service | URL | Description |
|---------|-----|-------------|
| **Kafka UI** | http://localhost:8888 | Kafka cluster management |
| **Gravitino** | http://localhost:8090 | Universal metadata management |
| **Gravitino REST** | http://localhost:9001 | Iceberg REST service |
| **MinIO Console** | http://localhost:9002 | Object storage UI |
| **Spark Master** | http://localhost:8080 | Spark cluster UI |
| **PostgreSQL** | localhost:5432 | Database (user: gravitino, pass: gravitino123) |

## Health Check

Run the health check script to verify all services:

**Windows:**
```cmd
scripts\health-check-extended.bat
```

**Linux/Mac:**
```bash
./scripts/health-check-extended.sh
```

## PostgreSQL Connection

You can connect to PostgreSQL using any database client:

```
Host: localhost
Port: 5432
Database: gravitino
Username: gravitino
Password: gravitino123
```

## Kafka Management

Access Kafka UI at http://localhost:8888 to:
- View topics and partitions
- Browse messages
- Monitor consumer groups
- Manage schemas

## Gravitino with PostgreSQL

Gravitino now uses PostgreSQL for persistent metadata storage:
- All catalogs, schemas, and tables are stored in PostgreSQL
- Metadata survives container restarts
- Better performance and reliability
- ACID compliance for metadata operations

## Testing the Setup

1. **Check PostgreSQL**: Verify database connection
2. **Test Kafka UI**: Create a test topic
3. **Verify Gravitino**: Access the web UI and create a catalog
4. **Check Iceberg REST**: Test REST API endpoints

## Troubleshooting

### PostgreSQL Issues
```bash
# Check PostgreSQL logs
docker-compose logs postgres

# Verify database is ready
docker-compose exec postgres pg_isready -U gravitino -d gravitino
```

### Kafka UI Issues
```bash
# Check Kafka UI logs
docker-compose logs kafka-ui

# Restart Kafka UI
docker-compose restart kafka-ui
```

### Gravitino Issues
```bash
# Check Gravitino logs
docker-compose logs gravitino

# Verify PostgreSQL connection
docker-compose exec postgres psql -U gravitino -d gravitino -c "\dt"
```

## Data Persistence

The following data is now persistent:
- **PostgreSQL**: All Gravitino metadata
- **MinIO**: Object storage data
- **Kafka**: Topic data and configurations

## Next Steps

1. Create your first catalog in Gravitino
2. Set up Iceberg tables using the REST API
3. Ingest data using Kafka and process with Spark
4. Monitor everything through the web UIs