from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

PIPELINE_NAME = "bronze_to_silver_events"
expected_columns = {"user_id", "event_type", "product_id", "category", "price", "event_time", "device",}
system_columns = {"raw_value", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",}

spark = SparkSession.builder \
                    .appName("bronze-to-silver-incremental") \
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

# =========================
# 1. 建立 metadata table
# =========================
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.silver")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.silver.bronze_clean (
    user_id STRING,
    event_type STRING,
    product_id STRING,
    category STRING,
    price BIGINT,
    event_time TIMESTAMP,
    kafka_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (hours(event_time))
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.silver.bad_events (
    user_id STRING,
    event_type STRING,
    product_id STRING,
    category STRING,
    price BIGINT,
    event_time TIMESTAMP,
    kafka_timestamp TIMESTAMP,
    event_hash STRING,
    error_reasons STRING,
    created_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (hours(created_at))
""")

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.meta")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.meta.pipeline_state (
    pipeline_name STRING,
    last_snapshot_id BIGINT,
    updated_at TIMESTAMP
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.meta.pipeline_quality_summary (
    pipeline_name STRING,
    snapshot_id BIGINT,
    total_count BIGINT,
    valid_count BIGINT,
    bad_count BIGINT,
    duplicate_removed_count BIGINT,
    created_at TIMESTAMP
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.meta.schema_drift_summary (
    pipeline_name STRING,
    observed_columns ARRAY<STRING>,
    missing_columns ARRAY<STRING>,
    new_columns ARRAY<STRING>,
    checked_at TIMESTAMP
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.meta.schema_change_requests (
    pipeline_name STRING,
    column_name STRING,
    change_type STRING,
    status STRING,
    detected_at TIMESTAMP,
    approved_at TIMESTAMP,
    applied_at TIMESTAMP
)
USING iceberg;
""")

# =========================
# 2. 取得目前 Bronze 最新 snapshot
# =========================
current_snapshot_row = spark.sql("""
SELECT snapshot_id
FROM demo.bronze.kafka_events_hidden_partition.snapshots
ORDER BY committed_at DESC
LIMIT 1
""").collect()

if not current_snapshot_row:
    print("No snapshot found in demo.bronze.kafka_events_hidden_partition. Nothing to process.")
    spark.stop()
    raise SystemExit(0)

current_snapshot_id = current_snapshot_row[0]["snapshot_id"]

# =========================
# 3. 取得上次處理到的 snapshot
# =========================
state_rows = spark.sql(f"""
SELECT last_snapshot_id
FROM demo.meta.pipeline_state
WHERE pipeline_name = '{PIPELINE_NAME}'
ORDER BY updated_at DESC
LIMIT 1
""").collect()

last_snapshot_id = state_rows[0]["last_snapshot_id"] if state_rows else None

print(f"last_snapshot_id = {last_snapshot_id}")
print(f"current_snapshot_id = {current_snapshot_id}")

if last_snapshot_id == current_snapshot_id:
    print("No new snapshot to process.")
    spark.stop()
    raise SystemExit(0)

# =========================
# 4. 讀 Bronze 增量資料
# =========================
reader = spark.read \
              .format("iceberg")

if last_snapshot_id is not None:
    bronze_df = reader.option("start-snapshot-id", str(last_snapshot_id)) \
                      .option("end-snapshot-id", str(current_snapshot_id)) \
                      .load("demo.bronze.kafka_events_hidden_partition")

else:
    # 第一次跑：先讀整張 Bronze table
    bronze_df = spark.table("demo.bronze.kafka_events_hidden_partition")


if bronze_df.rdd.isEmpty():
    print("Incremental bronze data is empty. Updating state only.")
else:
    json_key_df = bronze_df.select(F.map_keys(F.from_json(F.col("raw_value"),T.MapType(T.StringType(), T.StringType()))).alias("json_keys"))

    observed_payload_columns = set(row["key"] for row in json_key_df.select(F.explode("json_keys").alias("key")).distinct().collect())
    missing_columns = list(expected_columns - observed_payload_columns)
    new_columns = list(observed_payload_columns - expected_columns)
    if new_columns:
        print(f"Schema drift detected: new columns = {new_columns}")
        schema_drift_schema = T.StructType([
                                                T.StructField("pipeline_name", T.StringType(), False),
                                                T.StructField("observed_columns", T.ArrayType(T.StringType()), True),
                                                T.StructField("missing_columns", T.ArrayType(T.StringType()), True),
                                                T.StructField("new_columns", T.ArrayType(T.StringType()), True),
                                            ])
        schema_drift_df = spark.createDataFrame(
                                                    [(
                                                        PIPELINE_NAME,
                                                        sorted(list(observed_payload_columns)),
                                                        sorted(list(missing_columns)),
                                                        sorted(list(new_columns)),
                                                    )],
                                                    schema=schema_drift_schema,
                                                ) \
                               .withColumn("checked_at", F.current_timestamp())
        schema_drift_df.writeTo("demo.meta.schema_drift_summary").append()

        change_df = spark.createDataFrame(
                                            [(PIPELINE_NAME, c, "new_column", "pending") for c in new_columns],
                                            ["pipeline_name", "column_name", "change_type", "status"],
                                         ) \
                                         .withColumn("detected_at", F.current_timestamp()) \
                                         .withColumn("approved_at", F.lit(None).cast("timestamp")) \
                                         .withColumn("applied_at", F.lit(None).cast("timestamp"))
        change_df.createOrReplaceTempView("schema_change_updates")
        spark.sql("""
        MERGE INTO demo.meta.schema_change_requests t
        USING schema_change_updates s
        ON t.pipeline_name = s.pipeline_name
        AND t.column_name = s.column_name
        AND t.change_type = s.change_type
        WHEN NOT MATCHED THEN INSERT *
        """)                        

    payload_schema = T.StructType() \
                      .add("user_id", T.StringType()) \
                      .add("event_type", T.StringType()) \
                      .add("product_id", T.StringType()) \
                      .add("category", T.StringType()) \
                      .add("price", T.LongType()) \
                      .add("event_time", T.StringType()) \
                      .add("device", T.StringType())
    bronze_with_payload_df = bronze_df.withColumn("payload", F.from_json(F.col("raw_value"), payload_schema))    
    silver_df = bronze_with_payload_df.select(
                                                F.col("payload.user_id").alias("user_id"),
                                                F.col("payload.event_type").alias("event_type"),
                                                F.col("payload.product_id").alias("product_id"),
                                                F.col("payload.category").alias("category"),
                                                F.col("payload.price").cast("bigint").alias("price"),
                                                F.to_timestamp(F.col("payload.event_time")).alias("event_time"),
                                                F.col("kafka_timestamp"),
                                                F.col("payload.device").alias("device"),
                                            ) \
                                    .withColumn("event_hash",
                                        F.sha2(
                                            F.concat_ws(
                                                "||",
                                                F.coalesce(F.col("user_id"), F.lit("__NULL__")),
                                                F.coalesce(F.col("event_type"), F.lit("__NULL__")),
                                                F.coalesce(F.col("product_id"), F.lit("__NULL__")),
                                                F.coalesce(F.col("category"), F.lit("__NULL__")),
                                                F.coalesce(F.col("price").cast("string"), F.lit("__NULL__")),
                                                F.coalesce(F.col("event_time").cast("string"), F.lit("__NULL__")),
                                                F.coalesce(F.col("device"), F.lit("__NULL__")),
                                            ),
                                            256,
                                        )
                                    )
    
    dedup_df = silver_df.dropDuplicates(["event_hash"])

    allowed_event_types = ["view", "click", "purchase", "add_to_cart"]

    dedup_df = dedup_df.withColumn(
                                      "error_reasons",
                                      F.expr("""
                                                filter(array(
                                                    IF(event_time IS NULL, 'event_time_is_null', NULL),
                                                    IF(price IS NULL, 'price_is_null', NULL),
                                                    IF(price < 0, 'price_is_negative', NULL),
                                                    IF(event_type IS NULL, 'event_type_is_null', NULL),
                                                    IF(NOT event_type IN ('view', 'click', 'purchase', 'add_to_cart'), 'invalid_event_type', NULL)
                                                ), x -> x IS NOT NULL)
                                            """)
                                  )
    
    valid_df = dedup_df.filter(F.size(F.col("error_reasons")) == 0) \
                       .drop("error_reasons")

    bad_df = dedup_df.filter(F.size(F.col("error_reasons")) > 0) \
                     .withColumn("created_at", F.current_timestamp()) \
                     .withColumn("quarantine_at",F.current_timestamp()) \
                     .withColumn("is_replayed",F.lit(False)) \
                     .withColumn("replayed_at", F.lit(None).cast("timestamp")) \
                     .withColumn("replay_batch_id", F.lit(None).cast("string")) \
                     .select("user_id", "event_type", "product_id", "category", "price", "event_time", "device", "kafka_timestamp", "event_hash",
                             "created_at", "quarantine_at", "is_replayed", "replayed_at", "replay_batch_id", "error_reasons")

    valid_df.createOrReplaceTempView("silver_incremental")

    spark.sql("""
    MERGE INTO demo.silver.bronze_clean t
    USING silver_incremental s
    ON t.event_hash = s.event_hash
    WHEN NOT MATCHED THEN INSERT *
    """)

    bad_df.writeTo("demo.silver.bad_events").append()

    raw_count = silver_df.count()
    dedup_count = dedup_df.count()
    valid_count = valid_df.count()
    bad_count = bad_df.count()

    quality_df = spark.createDataFrame(
        [(
            PIPELINE_NAME,
            current_snapshot_id,
            raw_count,
            valid_count,
            bad_count,
            raw_count - dedup_count,
        )],
        [
            "pipeline_name",
            "snapshot_id",
            "total_count",
            "valid_count",
            "bad_count",
            "duplicate_removed_count",
        ],
    ).withColumn("created_at", F.current_timestamp())

    quality_df.writeTo("demo.meta.pipeline_quality_summary").append()

    spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.meta.pipeline_partitions (
        pipeline_name STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        processed BOOLEAN,
        updated_at TIMESTAMP
    )
    USING iceberg
    """)

    affected_partitions_df = valid_df.withColumn("start_time", F.date_trunc("hour", F.col("event_time"))) \
                                     .withColumn("end_time", F.expr("start_time + INTERVAL 1 HOUR")) \
                                     .select("start_time", "end_time") \
                                     .distinct() \
                                     .withColumn("pipeline_name", F.lit("silver_to_gold_events")) \
                                     .withColumn("processed", F.lit(False)) \
                                     .withColumn("updated_at", F.current_timestamp()) \
                                     .select("pipeline_name","start_time","end_time","processed","updated_at",)


    affected_partitions_df.writeTo("demo.meta.pipeline_partitions").append()

# 最後一定要更新 bronze_to_silver 的 snapshot state
state_update_df = spark.sql(f"""
SELECT
    '{PIPELINE_NAME}' AS pipeline_name,
    CAST({current_snapshot_id} AS BIGINT) AS last_snapshot_id,
    current_timestamp() AS updated_at
""")

state_update_df.writeTo("demo.meta.pipeline_state").append()

print(f"Pipeline state updated to snapshot_id = {current_snapshot_id}")

spark.stop()