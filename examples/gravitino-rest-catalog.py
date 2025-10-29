#!/usr/bin/env python3
"""
Gravitino Iceberg REST Catalog - Optimized Configuration
Based on Apache Gravitino 1.0.0 documentation
"""

from pyspark.sql import SparkSession
import time

print("🚀 Gravitino Iceberg REST Catalog Test")
print("=" * 60)

# 1. Check Gravitino REST service
try:
    import requests
    # Check Gravitino server version
    gravitino_resp = requests.get("http://gravitino:8090/api/version", timeout=5)
    print(f"✅ Gravitino Server: {gravitino_resp.json().get('version', 'Unknown')}")
    
    # Check Iceberg REST service endpoint
    rest_resp = requests.get("http://gravitino:9001/iceberg/v1/config", timeout=5)
    print(f"✅ Iceberg REST Service: Available")
    print(f"   Config: {rest_resp.json()}")
    
except Exception as e:
    print(f"❌ Service check failed: {e}")
    print("💡 Falling back to Hadoop catalog...")
    
print("⚡ Creating Spark session with Gravitino REST catalog...")

# GRAVITINO REST CATALOG CONFIG - Based on official docs
# URI: http://gravitino:9001/iceberg/ (as per documentation)
spark = SparkSession.builder \
    .appName("GravitinoRestCatalog") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.rest.type", "rest") \
    .config("spark.sql.catalog.rest.uri", "http://gravitino:9001/iceberg/") \
    .config("spark.sql.defaultCatalog", "rest") \
    .getOrCreate()

print("✅ Spark session with REST catalog created!")

# 2. Test basic operations
print("🚌 Testing Gravitino REST catalog...")

try:
    # Switch to REST catalog (as shown in docs)
    spark.sql("USE rest")
    print("✅ Using REST catalog")
    
    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS demo")
    print("✅ Database created")
    
    # Create table (following documentation example)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.gravitino_test (
            id bigint COMMENT 'unique id',
            name string,
            value double,
            created_at timestamp
        ) USING iceberg
    """)
    print("✅ Table created")
    
    # Insert data
    spark.sql("""
        INSERT INTO demo.gravitino_test VALUES 
        (1, 'rest_catalog', 100.0, current_timestamp()),
        (2, 'gravitino_server', 95.5, current_timestamp()),
        (3, 'iceberg_rest', 98.2, current_timestamp())
    """)
    print("✅ Data inserted")
    
    # Query data
    count = spark.sql("SELECT COUNT(*) FROM demo.gravitino_test").collect()[0][0]
    print(f"📊 Total records: {count}")
    
    # Show data
    print("📋 Sample data:")
    spark.sql("SELECT id, name, ROUND(value, 1) as value FROM demo.gravitino_test ORDER BY id").show()
    
    # Extended table info (as in docs)
    print("📝 Table details:")
    spark.sql("DESCRIBE TABLE EXTENDED demo.gravitino_test").show(20, False)
    
    print("✅ Gravitino REST catalog test successful!")
    
except Exception as e:
    print(f"❌ REST catalog error: {e}")
    print("🔄 Trying fallback to Hadoop catalog...")
    
    # Fallback to Hadoop catalog if REST fails
    try:
        spark.stop()
        
        # Create fallback session with Hadoop catalog
        spark_fallback = SparkSession.builder \
            .appName("FallbackHadoop") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "hadoop") \
            .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.defaultCatalog", "iceberg") \
            .getOrCreate()
        
        spark_fallback.sql("CREATE TABLE IF NOT EXISTS fallback_test (id int, name string) USING iceberg")
        spark_fallback.sql("INSERT INTO fallback_test VALUES (1, 'fallback')")
        fallback_count = spark_fallback.sql("SELECT COUNT(*) FROM fallback_test").collect()[0][0]
        print(f"📊 Fallback records: {fallback_count}")
        spark_fallback.stop()
        
    except Exception as fallback_error:
        print(f"❌ Fallback also failed: {fallback_error}")

print("\n🎯 Configuration Summary:")
print("=" * 60)
print("📝 GRAVITINO REST CATALOG:")
print("   - Type: Iceberg REST API")
print("   - URI: http://gravitino:9001/iceberg/")
print("   - Features:")
print("     ✓ Standard Iceberg REST API")
print("     ✓ Multi-catalog support") 
print("     ✓ Centralized metadata management")
print("     ✓ Credential vending support")
print("     ✓ OAuth2 & HTTPS support")

print("\n💡 Key benefits:")
print("   - Simplified client configuration")
print("   - Server-side storage management")
print("   - Multi-backend support (Hive, JDBC)")
print("   - Enterprise security features")

print("\n📚 Documentation reference:")
print("   - Official docs: https://gravitino.apache.org/docs/iceberg-rest-service")
print("   - REST API spec: Apache Iceberg REST API")

if 'spark' in locals():
    spark.stop()
print("✅ Test completed!")