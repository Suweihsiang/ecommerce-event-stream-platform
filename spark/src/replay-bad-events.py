from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REPLAY_BATCH_ID = datetime.now(timezone.utc).strftime("replay_%Y%m%d%H%M%S")

spark = SparkSession.builder \
                    .appName("replay-bad-events") \
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

bad_df = spark.sql("""
SELECT *
FROM demo.silver.bad_events
WHERE is_replayed = false
AND size(error_reasons) > 0
""")

candidate_count = bad_df.count()

if candidate_count == 0:
    print("No bad events to replay.")
    spark.stop()
    raise SystemExit(0)

summary_rows = bad_df.select(F.explode("error_reasons").alias("error_reason")) \
                     .groupBy("error_reason") \
                     .count() \
                     .collect()

rules_df = spark.table("demo.meta.repair_rules") \
                .filter("enabled = true") \
                .filter("auto_replay = true")
rules = {row["error_reason"]: row for row in rules_df.collect()}

repair_df = bad_df.withColumnRenamed("event_hash", "original_event_hash")
if "invalid_event_type" in rules:
    repair_df = repair_df.withColumn("event_type",F.when(
                                                            F.array_contains(F.col("error_reasons"), "invalid_event_type"),
                                                            F.lit(rules["invalid_event_type"]["action_value"])
                                                        ).otherwise(F.col("event_type"))
                                    )

if "price_is_negative" in rules:
    repair_df = repair_df.withColumn("price",F.when(
                                                       F.array_contains(F.col("error_reasons"), "price_is_negative"),
                                                       F.abs(F.col("price"))
                                                   ).otherwise(F.col("price"))
                                    )

if "event_time_is_null" in rules:
    repair_df = repair_df.withColumn("event_time",F.when(
                                                            F.array_contains(F.col("error_reasons"), "event_time_is_null"),
                                                            F.col(rules["event_time_is_null"]["action_value"])
                                                        ).otherwise(F.col("event_time"))
                                    )

repair_df = repair_df.withColumn("event_hash",F.sha2(
                                                        F.concat_ws(
                                                            "||",
                                                            F.coalesce(F.col("user_id"), F.lit("__NULL__")),
                                                            F.coalesce(F.col("event_type"), F.lit("__NULL__")),
                                                            F.coalesce(F.col("product_id"), F.lit("__NULL__")),
                                                            F.coalesce(F.col("category"), F.lit("__NULL__")),
                                                            F.coalesce(F.col("price").cast("string"), F.lit("__NULL__")),
                                                            F.coalesce(F.col("event_time").cast("string"), F.lit("__NULL__")),
                                                            F.coalesce(F.col("device").cast("string"), F.lit("__NULL__")),
                                                        ),
                                                        256,
                                                    )
                                )

repair_clean_df = repair_df.select("user_id", "event_type", "product_id", "category", "price", "event_time", "device", "kafka_timestamp", "event_hash")
repair_clean_df = repair_clean_df.dropDuplicates(["event_hash"])

repair_clean_df.createOrReplaceTempView("repair_events_clean")

spark.sql("""
MERGE INTO demo.silver.bronze_clean t
USING repair_events_clean s
ON t.event_hash = s.event_hash
WHEN NOT MATCHED THEN INSERT *
""")

affected_ranges_df = repair_clean_df.withColumn("start_time", F.date_trunc("hour", F.col("event_time"))) \
                                    .withColumn("end_time", F.expr("start_time + INTERVAL 1 HOUR")) \
                                    .select("start_time", "end_time") \
                                    .distinct() \
                                    .withColumn("pipeline_name", F.lit("silver_to_gold_events")) \
                                    .withColumn("processed", F.lit(False)) \
                                    .withColumn("updated_at", F.current_timestamp()) \
                                    .select("pipeline_name","start_time","end_time","processed","updated_at")

affected_ranges_df.writeTo("demo.meta.pipeline_partitions").append()

repair_df.select("original_event_hash").distinct().createOrReplaceTempView("repair_original_hashes")

spark.sql(f"""
MERGE INTO demo.silver.bad_events t
USING repair_original_hashes s
ON t.event_hash = s.original_event_hash
WHEN MATCHED THEN UPDATE SET
    t.is_replayed = true,
    t.replayed_at = current_timestamp(),
    t.replay_batch_id = '{REPLAY_BATCH_ID}'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.meta.replay_quality_summary (
    replay_batch_id STRING,
    source_error_reason STRING,
    candidate_count BIGINT,
    repaired_count BIGINT,
    skipped_count BIGINT,
    created_at TIMESTAMP
)
USING iceberg
""")

summary_data = [(REPLAY_BATCH_ID, row["error_reason"], row["count"], row["count"], 0) for row in summary_rows]

summary_df = spark.createDataFrame(summary_data,["replay_batch_id", "source_error_reason", "candidate_count", "repaired_count", "skipped_count",]) \
                  .withColumn("created_at",F.current_timestamp())

summary_df.writeTo("demo.meta.replay_quality_summary").append()

print(f"Replay finished. batch_id={REPLAY_BATCH_ID}, candidate_count={candidate_count}")

spark.stop()