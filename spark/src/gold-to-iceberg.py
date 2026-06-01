from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

PIPELINE_NAME = "gold_to_iceberg_events"

spark = SparkSession.builder.appName("gold-to-iceberg") \
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

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.gold")
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.meta")

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

partitions = spark.sql(f"""
SELECT DISTINCT event_time
FROM demo.meta.pipeline_partitions
WHERE pipeline_name = '{PIPELINE_NAME}'
  AND processed = false
ORDER BY event_time
""").collect()

if not partitions:
    print("No pending partitions for gold_to_iceberg.")
    spark.stop()
    raise SystemExit(0)


def parquet_partition_to_iceberg(dataset_name: str, create_sql: str, dt: str, hour: str):
    path = f"s3a://spark-data/gold/{dataset_name}/dt={dt}/hour={hour}/"
    table = f"demo.gold.{dataset_name}"

    print(f"Loading {path}")

    df = spark.read \
              .parquet(path) \
              .withColumn("dt", lit(dt)) \
              .withColumn("hour", lit(hour))
    

    print(f"Writing {table}")

    spark.sql(create_sql)

    df.writeTo(table) \
      .overwritePartitions()


for row in partitions:
    dt = row["dt"]
    hour = row["hour"].zfill(2)

    print(f"Processing gold_to_iceberg partition dt={dt}, hour={hour}")

    parquet_partition_to_iceberg(
        "event_type_5min_count",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.event_type_5min_count (
            event_type STRING,
            count BIGINT,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            dt STRING,
            hour STRING
        )
        USING iceberg
        PARTITIONED BY (dt, hour)
        """,
        dt,
        hour,
    )

    parquet_partition_to_iceberg(
        "category_5min_count",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.category_5min_count (
            category STRING,
            count BIGINT,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            dt STRING,
            hour STRING
        )
        USING iceberg
        PARTITIONED BY (dt, hour)
        """,
        dt,
        hour,
    )

    parquet_partition_to_iceberg(
        "revenue_5min",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.revenue_5min (
            revenue BIGINT,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            dt STRING,
            hour STRING
        )
        USING iceberg
        PARTITIONED BY (dt, hour)
        """,
        dt,
        hour,
    )

    parquet_partition_to_iceberg(
        "active_user_5min",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.active_user_5min (
            active_user_count BIGINT,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            dt STRING,
            hour STRING
        )
        USING iceberg
        PARTITIONED BY (dt, hour)
        """,
        dt,
        hour,
    )

    spark.sql(f"""
    UPDATE demo.meta.pipeline_partitions
    SET processed = true,
        updated_at = current_timestamp()
    WHERE pipeline_name = '{PIPELINE_NAME}'
      AND dt = '{dt}'
      AND hour = '{hour}'
    """)
    
    print(f"Finished gold_to_iceberg partition dt={dt}, hour={hour}")

spark.stop()