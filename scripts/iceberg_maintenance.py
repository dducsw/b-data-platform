from pyspark.sql import SparkSession
from datetime import datetime, timedelta, timezone

# ================== CONFIG ==================
APP_NAME = "IcebergMaintenance"

TABLE = "iceberg.bronze.buswaypoint" # điền tên bảng cần bảo trì vào đây, vd bronze.buswaypoint

CATALOG = "iceberg"

# Iceberg REST catalog endpoint
ICEBERG_REST_URI = "http://iceberg-rest:8181"

WAREHOUSE = "s3a://lake/"
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "minioadmin"   
S3_SECRET_KEY = "minioadmin123"  

ENABLE_COMPACT_DATA_FILES   = True   # gom file nhỏ -> file lớn
ENABLE_EXPIRE_SNAPSHOTS     = True   # xóa snapshot cũ
ENABLE_REMOVE_ORPHANS       = True   # xóa file mồ côi
ENABLE_REWRITE_MANIFESTS    = True   # gom manifest nhỏ

MIN_FILE_SIZE_BYTES     = 3 * 1024 * 1024       
TARGET_FILE_SIZE_BYTES  = 128 * 1024 * 1024     
MAX_FILE_SIZE_BYTES     = 256 * 1024 * 1024   
RETAIN_LAST_SNAPSHOTS   = 3   
ORPHAN_RETENTION_DAYS   = 3   # chỉ xóa file cũ hơn 3 ngày

spark = (
    SparkSession.builder
    .appName(APP_NAME)

    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
    .config(f"spark.sql.catalog.{CATALOG}.uri", ICEBERG_REST_URI)
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def run(sql: str):
    print("\n=== RUN SQL ===")
    print(sql.strip())
    res = spark.sql(sql)
    print("=== RESULT ===")
    res.show(truncate=False)


# ================== MAINTENANCE TASKS ==================
print("\n=== Row count hiện tại ===")
spark.sql(f"SELECT COUNT(*) AS row_count FROM {TABLE}").show()

print("\n=== Snapshots hiện tại (nếu có) ===")
spark.sql(f"""
    SELECT committed_at, snapshot_id, parent_id, operation
    FROM {TABLE}.snapshots
""").show(truncate=False)

def compact_data_files():
    """
    Gom các data file nhỏ thành file lớn hơn.
    """
    if not ENABLE_COMPACT_DATA_FILES:
        print("Skip compact_data_files (disabled).")
        return
    print("\n>>> [1] Compact small data files")
    sql = f"""
        CALL {CATALOG}.system.rewrite_data_files(
            table => '{TABLE}',
            options => map(
                'min-file-size-bytes',    '{MIN_FILE_SIZE_BYTES}',
                'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                'max-file-size-bytes',    '{MAX_FILE_SIZE_BYTES}'
            )
        )
    """
    run(sql)


def rewrite_manifests():
    """
    Gom manifest nhỏ, sắp xếp lại metadata để query nhanh hơn.
    """
    if not ENABLE_REWRITE_MANIFESTS:
        print("Skip rewrite_manifests (disabled).")
        return

    print("\n>>> [2] Rewrite manifests")
    sql = f"""
        CALL {CATALOG}.system.rewrite_manifests(
            table => '{TABLE}'
        )
    """
    run(sql)


def expire_snapshots():
    """
    Xóa snapshot cũ, chỉ giữ lại một số snapshot gần nhất (expire_snapshots).
    """
    if not ENABLE_EXPIRE_SNAPSHOTS:
        print("Skip expire_snapshots (disabled).")
        return

    print("\n>>> [3] Expire old snapshots")
    sql = f"""
        CALL {CATALOG}.system.expire_snapshots(
            table => '{TABLE}',
            retain_last => {RETAIN_LAST_SNAPSHOTS}
        )
    """
    run(sql)


def remove_orphan_files():
    """
    Xóa orphan files – file không còn được snapshot nào tham chiếu.
    """
    if not ENABLE_REMOVE_ORPHANS:
        print("Skip remove_orphan_files (disabled).")
        return

    print("\n>>> [4] Remove orphan files")
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=ORPHAN_RETENTION_DAYS)
    ).strftime("%Y-%m-%d %H:%M:%S")

    sql = f"""
        CALL {CATALOG}.system.remove_orphan_files(
            table => '{TABLE}',
            older_than => TIMESTAMP '{cutoff}'
        )
    """
    run(sql)


# ================== MAIN ==================
if __name__ == "__main__":
    try:

        compact_data_files()
        rewrite_manifests()
        expire_snapshots()
        remove_orphan_files()
    finally:
        spark.stop()
        print("\n>>> Spark session stopped.")
