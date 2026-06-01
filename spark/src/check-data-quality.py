from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import os
import requests

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

spark = SparkSession.builder \
                    .appName("check-data-quality") \
                    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
                    .config("spark.sql.catalog.demo.type", "hadoop") \
                    .config("spark.sql.catalog.demo.warehouse", "s3a://spark-data/iceberg-warehouse/") \
                    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
                    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
                    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.minio.svc.cluster.local:9000") \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")

def send_telegram_message(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram env not set, skip alert.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    resp = requests.post(
        url,
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
        },
        timeout=10,
    )

    if resp.status_code != 200:
        raise RuntimeError(f"Telegram alert failed: {resp.text}")

# =========================
# 1. 建立 metadata table
# =========================
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.meta")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.meta.data_quality_metrics (
    pipeline_name STRING,
    snapshot_id STRING,

    total_count BIGINT,
    valid_count BIGINT,
    bad_count BIGINT,

    bad_rate DOUBLE,

    duplicate_removed_count BIGINT,
    duplicate_rate DOUBLE,

    created_at TIMESTAMP
)
USING iceberg;
""")

# =========================
# 2. 取得目前 Bronze 最新 snapshot
# =========================
latest_rows = spark.sql("""
SELECT *
FROM demo.meta.pipeline_quality_summary
ORDER BY created_at DESC
LIMIT 1
""").collect()

if not latest_rows:
    print("No pipeline quality summary found.")
    spark.stop()
    raise SystemExit(0)

latest = latest_rows[0]

total_count = latest["total_count"]
bad_count = latest["bad_count"]
duplicate_count = latest["duplicate_removed_count"]

bad_rate = bad_count / total_count if total_count else 0
duplicate_rate = duplicate_count / total_count if total_count else 0

metrics_df = spark.createDataFrame(
    [(
        latest["pipeline_name"],
        str(latest["snapshot_id"]),
        total_count,
        latest["valid_count"],
        bad_count,
        bad_rate,
        duplicate_count,
        duplicate_rate,
        datetime.now(),
    )],
    [
        "pipeline_name",
        "snapshot_id",
        "total_count",
        "valid_count",
        "bad_count",
        "bad_rate",
        "duplicate_removed_count",
        "duplicate_rate",
        "created_at",
    ],
)

metrics_df.writeTo("demo.meta.data_quality_metrics").append()

BAD_RATE_THRESHOLD = 0.05
DUPLICATE_RATE_THRESHOLD = 0.10

if bad_rate > BAD_RATE_THRESHOLD or duplicate_rate > DUPLICATE_RATE_THRESHOLD:
    message = f"""
🚨 Data Quality Alert

Pipeline: {latest["pipeline_name"]}
Snapshot: {latest["snapshot_id"]}

Total: {total_count}
Valid: {latest["valid_count"]}
Bad: {bad_count}
Bad Rate: {bad_rate:.2%}

Duplicate Removed: {duplicate_count}
Duplicate Rate: {duplicate_rate:.2%}

Time: {datetime.now()}
"""
    send_telegram_message(message)

spark.stop()