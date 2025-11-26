from pyspark.sql import SparkSession
from datetime import datetime, timedelta, timezone

# ================== CONFIG ==================
APP_NAME = "IcebergMaintenance"

# Tên bảng Iceberg bạn muốn bảo trì
TABLE = "iceberg.demo.bus_gps_demo"

# Tên catalog trong Spark (phù hợp với setup của bạn)
# Ví dụ: 'iceberg', 'hadoop', 'rest', ...
CATALOG = "iceberg"

# Bật / tắt từng loại maintenance
ENABLE_COMPACT_DATA_FILES   = True   # gom file nhỏ -> file lớn
ENABLE_EXPIRE_SNAPSHOTS     = True   # xóa snapshot cũ
ENABLE_REMOVE_ORPHANS       = True   # xóa file mồ côi
ENABLE_REWRITE_MANIFESTS    = True   # gom manifest nhỏ

# Tham số cho compact
MIN_FILE_SIZE_BYTES     = 1 * 1024 * 1024       # < 1MB thì gom
TARGET_FILE_SIZE_BYTES  = 128 * 1024 * 1024     # file mục tiêu ~128MB
MAX_FILE_SIZE_BYTES     = 256 * 1024 * 1024     # không quá 256MB

# Tham số expire snapshots
RETAIN_LAST_SNAPSHOTS   = 3   # giữ lại 3 snapshot mới nhất

# Tham số orphan files
ORPHAN_RETENTION_DAYS   = 2   # chỉ xóa file cũ hơn 2 ngày

# ================== SETUP SPARK ==================
spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def run(sql: str):
    """Helper để log SQL và show kết quả (nếu có)."""
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
    Gom các data file nhỏ thành file lớn hơn (rewrite_data_files).
    Dựa trên phần 'Compact data files' trong docs.
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


def expire_snapshots():
    """
    Xóa snapshot cũ, chỉ giữ lại một số snapshot gần nhất (expire_snapshots).
    Dựa trên phần 'Expire Snapshots' trong docs.
    """
    if not ENABLE_EXPIRE_SNAPSHOTS:
        print("Skip expire_snapshots (disabled).")
        return

    print("\n>>> [2] Expire old snapshots")
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
    Dựa trên phần 'Delete orphan files' trong docs.
    """
    if not ENABLE_REMOVE_ORPHANS:
        print("Skip remove_orphan_files (disabled).")
        return

    print("\n>>> [3] Remove orphan files")
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


def rewrite_manifests():
    """
    Gom manifest nhỏ, sắp xếp lại metadata để query nhanh hơn
    (rewriteManifests trong docs).
    """
    if not ENABLE_REWRITE_MANIFESTS:
        print("Skip rewrite_manifests (disabled).")
        return

    print("\n>>> [4] Rewrite manifests")
    sql = f"""
        CALL {CATALOG}.system.rewrite_manifests(
            table => '{TABLE}'
        )
    """
    run(sql)


# ================== MAIN ==================

if __name__ == "__main__":
    try:
        # Thứ tự gợi ý:
        # 1. Compact data files (gom file nhỏ)
        # 2. Rewrite manifests (gom metadata sau khi data ổn định)
        # 3. Expire snapshots (dọn snapshot cũ)
        # 4. Remove orphan files (dọn file lẻ không còn dùng)
        compact_data_files()
        rewrite_manifests()
        expire_snapshots()
        remove_orphan_files()
    finally:
        spark.stop()
        print("\n>>> Spark session stopped.")
