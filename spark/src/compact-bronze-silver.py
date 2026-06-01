from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName("compact-bronze-silver") \
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

table_names = ["demo.bronze.kafka_events_hidden_partition","demo.silver.bronze_clean"]

for table_name in table_names:
  print(f"{table_name} before compact:")
  spark.sql(f"""
  SELECT
    count(*) AS file_count,
    sum(record_count) AS total_records,
    sum(file_size_in_bytes) AS total_bytes
  FROM {table_name}.files
  """).show(truncate=False)

  print(f"Compacting {table_name}")

  spark.sql(f"""
  CALL demo.system.rewrite_data_files(
    table => '{table_name}'
  )
  """).show(truncate=False)

  print(f"{table_name} after compact:")
  spark.sql(f"""
  SELECT
    count(*) AS file_count,
    sum(record_count) AS total_records,
    sum(file_size_in_bytes) AS total_bytes
  FROM {table_name}.files
  """).show(truncate=False)

  spark.sql(f"""
  SELECT
    file_path,
    record_count,
    file_size_in_bytes
  FROM {table_name}.files
  ORDER BY file_size_in_bytes DESC
  LIMIT 20
  """).show(truncate=False)

spark.stop()