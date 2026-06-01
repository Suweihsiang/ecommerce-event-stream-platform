from pyspark.sql import SparkSession
from pyspark.sql import functions  as F
from pyspark.sql import types as T

schema = T.StructType() \
          .add("user_id", T.StringType()) \
          .add("event_type", T.StringType()) \
          .add("product_id", T.StringType()) \
          .add("category", T.StringType()) \
          .add("price", T.IntegerType()) \
          .add("event_time", T.StringType())

spark = SparkSession.builder \
                    .appName("kafka-to-minio-bronze-iceberg") \
                    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
                    .config("spark.sql.catalog.demo.type", "hadoop") \
                    .config("spark.sql.catalog.demo.warehouse", "s3a://spark-data/iceberg-warehouse/") \
                    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
                    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
                    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.minio.svc.cluster.local:9000") \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bronze")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.kafka_events_hidden_partition (
    user_id STRING,
    event_type STRING,
    product_id STRING,
    category STRING,
    price BIGINT,
    event_time TIMESTAMP,
    kafka_timestamp TIMESTAMP,
    raw_value STRING,
    kafka_topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT
)
USING iceberg
PARTITIONED BY (hours(event_time))
""")

df = spark.readStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "kafka.kafka.svc.cluster.local:9092") \
          .option("subscribe", "test-topic") \
          .option("startingOffsets", "latest") \
          .option("failOnDataLoss", "false") \
          .load()

df_parsed = df.select(
                        F.col("topic").alias("kafka_topic"),
                        F.col("partition").alias("kafka_partition"),
                        F.col("offset").alias("kafka_offset"),
                        F.col("timestamp").alias("kafka_timestamp"),
                        F.col("value").cast("string").alias("raw_value"),
                    ) \
              .withColumn("json", F.from_json(F.col("raw_value"), schema)) \
              .select(
                          F.col("json.user_id"),
                          F.col("json.event_type"),
                          F.col("json.product_id"),
                          F.col("json.category"),
                          F.col("json.price").cast("bigint").alias("price"),
                          F.to_timestamp(F.col("json.event_time")).alias("event_time"),
                          "kafka_timestamp",
                          "raw_value",
                          "kafka_topic",
                          "kafka_partition",
                          "kafka_offset",
                      )

bronze_df = df_parsed.select(
                                "user_id",
                                "event_type",
                                "product_id",
                                "category",
                                "price",
                                "event_time",
                                "kafka_timestamp",
                                "raw_value",
                                "kafka_topic",
                                "kafka_partition",
                                "kafka_offset",
                            )

bronze_query = bronze_df.writeStream \
                        .format("iceberg") \
                        .outputMode("append") \
                        .option("checkpointLocation", "s3a://spark-data/checkpoints/bronze_iceberg/") \
                        .trigger(processingTime="1 minute") \
                        .toTable("demo.bronze.kafka_events_hidden_partition")

bronze_query.awaitTermination()