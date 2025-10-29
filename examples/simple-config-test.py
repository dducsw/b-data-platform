#!/usr/bin/env python3

"""
Simplified Gravitino + Iceberg Test with REST Catalog
"""

import requests
from pyspark.sql import SparkSession
from datetime import datetime

print("🚀 Simplified Gravitino Test")
print("=" * 40)

# Quick Gravitino check
try:
    resp = requests.get("http://gravitino:8090/api/version", timeout=5)
    print(f"✅ Gravitino: {resp.json()['version']['version']}")
except:
    print("⚠️ Gravitino connection issue")

# Ultra-simplified Spark session
print("⚡ Creating minimal Spark session...")
spark = SparkSession.builder \
    .appName("SimpleGravitinoTest") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("✅ Spark ready!")

# Simple test
print("🚌 Quick test...")
try:
    spark.sql("CREATE TABLE IF NOT EXISTS iceberg.quick_test (id INT, name STRING) USING iceberg")
    spark.sql("INSERT INTO iceberg.quick_test VALUES (1, 'test'), (2, 'demo')")
    result = spark.sql("SELECT * FROM iceberg.quick_test")
    print(f"📊 Records: {result.count()}")
    result.show()
    print("✅ Test successful!")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n🎯 Configuration comparison:")
print("=" * 50)
print("📝 CURRENT (Hadoop catalog):")
print("   - Lines: ~15 configs")
print("   - Type: hadoop catalog")
print("   - S3: Direct configuration")
print("   - Metadata: File-based")

print("\n📝 REST CATALOG option:")
print("   - Lines: ~8 configs") 
print("   - Type: rest catalog")
print("   - S3: Via catalog properties")
print("   - Metadata: Gravitino managed")

print("\n💡 To use REST catalog, update to:")
print("""
spark = SparkSession.builder \\
    .appName("RestCatalogTest") \\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\
    .config("spark.sql.catalog.gravitino", "org.apache.iceberg.spark.SparkCatalog") \\
    .config("spark.sql.catalog.gravitino.type", "rest") \\
    .config("spark.sql.catalog.gravitino.uri", "http://gravitino:8090/api/iceberg/") \\
    .config("spark.sql.catalog.gravitino.warehouse", "s3a://warehouse") \\
    .config("spark.sql.defaultCatalog", "gravitino") \\
    .getOrCreate()
""")

spark.stop()