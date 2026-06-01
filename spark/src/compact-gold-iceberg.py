from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName("compact-gold-iceberg") \
                    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
                    .config("spark.sql.catalog.demo.type", "hadoop") \
                    .config("spark.sql.catalog.demo.warehouse", "s3a://spark-data/iceberg-warehouse/") \
                    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
                    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
                    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.minio.svc.cluster.local:9000") \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.sql.shuffle.partitions", "4") \
                    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")

tables = [
    "gold.event_type_5min_count",
    "gold.category_5min_count",
    "gold.revenue_5min",
    "gold.active_user_5min",
]

for table in tables:
    print(f"Compacting {table}")

    spark.sql(f"""
    CALL demo.system.rewrite_data_files(
      table => '{table}'
    )
    """).show(truncate=False)

spark.stop()