from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName("cleanup-iceberg") \
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

tables = [
    "bronze.kafka_events_hidden_partition",
    "silver.bronze_clean",
    "gold.event_type_5min_count",
    "gold.category_5min_count",
    "gold.revenue_5min",
    "gold.active_user_5min",
]

for table in tables:
    print(f"Expiring snapshots for {table}")

    spark.sql(f"""
    CALL demo.system.expire_snapshots(
      table => '{table}',
      retain_last => 3
    )
    """).show(truncate=False)

    print(f"Removing orphan files for {table}")

    spark.sql(f"""
    CALL demo.system.remove_orphan_files(
      table => '{table}'
    )
    """).show(truncate=False)

spark.stop()