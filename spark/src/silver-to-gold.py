from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PIPELINE_NAME = "silver_to_gold_events"

spark = SparkSession.builder \
                    .appName("silver-to-gold") \
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

rows = spark.sql(f"""
SELECT DISTINCT start_time, end_time
FROM demo.meta.pipeline_partitions
WHERE pipeline_name = '{PIPELINE_NAME}'
  AND processed = false
ORDER BY start_time, end_time
""").collect()

if not rows:
    print("No pending time ranges for silver_to_gold.")
    spark.stop()
    raise SystemExit(0)

def parquet_partition_to_iceberg(df, dataset_name: str, create_sql: str):
    table = f"demo.gold.{dataset_name}"

    print(f"Writing {table}")

    spark.sql(create_sql)

    df.writeTo(table).overwritePartitions()


for row in rows:
    start_time = row["start_time"]
    end_time = row["end_time"]

    df = spark.table("demo.silver.bronze_clean") \
              .filter(F.col("event_time") >= F.lit(start_time)) \
              .filter(F.col("event_time") < F.lit(end_time)) \
              .filter(F.col("event_time").isNotNull())


    event_type_count_df = df.filter(F.col("event_type").isNotNull()) \
                            .groupBy(F.window(F.col("event_time"), "5 minutes"), F.col("event_type")) \
                            .count() \
                            .withColumn("window_start", F.col("window.start")) \
                            .withColumn("window_end", F.col("window.end")) \
                            .drop("window")


    parquet_partition_to_iceberg(
        event_type_count_df,
        "event_type_5min_count",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.event_type_5min_count (
            event_type STRING,
            count BIGINT,
            window_start TIMESTAMP,
            window_end TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (hours(window_start))
        """
    )

    category_count_df = df.filter(F.col("category").isNotNull()) \
                            .groupBy(F.window(F.col("event_time"), "5 minutes"), F.col("category")) \
                            .count() \
                            .withColumn("window_start", F.col("window.start")) \
                            .withColumn("window_end", F.col("window.end")) \
                            .drop("window")


    parquet_partition_to_iceberg(
        category_count_df,
        "category_5min_count",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.category_5min_count (
            category STRING,
            count BIGINT,
            window_start TIMESTAMP,
            window_end TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (hours(window_start))
        """
    )

    revenue_df = df.filter(F.col("price").isNotNull()) \
                    .groupBy(F.window(F.col("event_time"), "5 minutes")) \
                    .agg(F.sum("price").alias("revenue")) \
                    .withColumn("window_start", F.col("window.start")) \
                    .withColumn("window_end", F.col("window.end")) \
                    .drop("window")


    parquet_partition_to_iceberg(
        revenue_df,
        "revenue_5min",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.revenue_5min (
            revenue BIGINT,
            window_start TIMESTAMP,
            window_end TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (hours(window_start))
        """
    )

    active_user_df = df.filter(F.col("user_id").isNotNull()) \
                        .groupBy(F.window(F.col("event_time"), "5 minutes")) \
                        .agg(F.countDistinct("user_id").alias("active_user_count")) \
                        .withColumn("window_start", F.col("window.start")) \
                        .withColumn("window_end", F.col("window.end")) \
                        .drop("window")


    parquet_partition_to_iceberg(
        active_user_df,
        "active_user_5min",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.active_user_5min (
            active_user_count BIGINT,
            window_start TIMESTAMP,
            window_end TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (hours(window_start))
        """
    )

    device_summary_df = df.withColumn("device",F.coalesce(F.col("device"),F.lit("unknown"))) \
                          .groupBy(F.window("event_time", "5 minutes"),F.col("device")) \
                          .agg(F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
                               F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
                               F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
                               F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
                               F.sum(F.when(F.col("event_type") == "purchase",F.col("price").cast("double")).otherwise(F.lit(0.0))).alias("revenue"),
                               F.countDistinct("user_id").alias("active_user_count")) \
                          .withColumn("start_time", F.col("window.start")) \
                          .withColumn("end_time", F.col("window.end")) \
                          .drop("window") \
                          .withColumn("ctr",F.when(F.col("view_count") > 0, F.col("click_count") / F.col("view_count")).otherwise(F.lit(0.0))) \
                          .withColumn("add_to_cart_rate",F.when(F.col("view_count") > 0, F.col("add_to_cart_count") / F.col("view_count")).otherwise(F.lit(0.0))) \
                          .withColumn("purchase_conversion_rate",F.when(F.col("view_count") > 0, F.col("purchase_count") / F.col("view_count")).otherwise(F.lit(0.0))) \
                          .withColumn("updated_at", F.current_timestamp()) \
                          .select("start_time","end_time","device","view_count","click_count","add_to_cart_count","purchase_count","ctr","add_to_cart_rate",
                                  "purchase_conversion_rate","revenue","active_user_count","updated_at",)
   
    parquet_partition_to_iceberg(
        device_summary_df,
        "device_5min_summary",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.device_5min_summary (
            start_time TIMESTAMP,
            end_time TIMESTAMP,

            device STRING,

            view_count BIGINT,
            click_count BIGINT,
            add_to_cart_count BIGINT,
            purchase_count BIGINT,

            ctr DOUBLE,
            add_to_cart_rate DOUBLE,
            purchase_conversion_rate DOUBLE,

            revenue DOUBLE,
            active_user_count BIGINT,

            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (hours(start_time))
        """
    )

    category_conversion_df = df.withColumn("category",F.coalesce(F.col("category"),F.lit("unknown"))) \
                          .groupBy(F.window("event_time", "5 minutes"),F.col("category")) \
                          .agg(F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
                               F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
                               F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
                               F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
                               F.sum(F.when(F.col("event_type") == "purchase",F.col("price").cast("double")).otherwise(F.lit(0.0))).alias("revenue"),
                               F.countDistinct("user_id").alias("active_user_count")) \
                          .withColumn("start_time", F.col("window.start")) \
                          .withColumn("end_time", F.col("window.end")) \
                          .drop("window") \
                          .withColumn("ctr",F.when(F.col("view_count") > 0, F.col("click_count") / F.col("view_count")).otherwise(F.lit(0.0))) \
                          .withColumn("add_to_cart_rate",F.when(F.col("view_count") > 0, F.col("add_to_cart_count") / F.col("view_count")).otherwise(F.lit(0.0))) \
                          .withColumn("purchase_conversion_rate",F.when(F.col("view_count") > 0, F.col("purchase_count") / F.col("view_count")).otherwise(F.lit(0.0))) \
                          .withColumn("updated_at", F.current_timestamp()) \
                          .select("start_time","end_time","category","view_count","click_count","add_to_cart_count","purchase_count","ctr","add_to_cart_rate",
                                  "purchase_conversion_rate","revenue","active_user_count","updated_at",)
   
    parquet_partition_to_iceberg(
        category_conversion_df,
        "category_conversion_5min",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.category_conversion_5min (
            start_time TIMESTAMP,
            end_time TIMESTAMP,

            category STRING,

            view_count BIGINT,
            click_count BIGINT,
            add_to_cart_count BIGINT,
            purchase_count BIGINT,

            ctr DOUBLE,
            add_to_cart_rate DOUBLE,
            purchase_conversion_rate DOUBLE,

            revenue DOUBLE,
            active_user_count BIGINT,

            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (hours(start_time))
        """
    )

    user_activity_df = df.groupBy(F.window("event_time", "5 minutes"),F.col("user_id")) \
                         .agg(F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
                              F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
                              F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
                              F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
                              F.sum(F.when(F.col("event_type") == "purchase",F.col("price").cast("double")).otherwise(F.lit(0.0))).alias("revenue"),
                              F.max("event_time").alias("last_active_time")) \
                         .withColumn("start_time", F.col("window.start")) \
                         .withColumn("end_time", F.col("window.end")) \
                         .drop("window") \
                         .withColumn("updated_at", F.current_timestamp()) \
                         .select("start_time","end_time","user_id","view_count","click_count","add_to_cart_count","purchase_count","revenue","last_active_time","updated_at",)
   
    parquet_partition_to_iceberg(
        user_activity_df,
        "user_activity_5min_summary",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.user_activity_5min_summary (
            start_time TIMESTAMP,
            end_time TIMESTAMP,

            user_id STRING,

            view_count BIGINT,
            click_count BIGINT,
            add_to_cart_count BIGINT,
            purchase_count BIGINT,

            revenue DOUBLE,

            last_active_time TIMESTAMP,

            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (hours(start_time))
        """
    )

    spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.gold.user_profile (
        user_id STRING,

        total_view BIGINT,
        total_click BIGINT,
        total_add_to_cart BIGINT,
        total_purchase BIGINT,

        total_revenue DOUBLE,

        favorite_category STRING,
        favorite_device STRING,

        last_active_time TIMESTAMP,

        updated_at TIMESTAMP
    )
    USING iceberg
    """)

    df = spark.table("demo.silver.bronze_clean")
    user_metrics_df = df.groupBy(F.col("user_id")) \
                        .agg(F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("total_view"),
                             F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("total_click"),
                             F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("total_add_to_cart"),
                             F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchase"),
                             F.sum(F.when(F.col("event_type") == "purchase",F.col("price").cast("double")).otherwise(F.lit(0.0))).alias("total_revenue"),
                             F.max("event_time").alias("last_active_time")) 
   
    user_category_df = df.groupBy("user_id", "category").count()
    category_window = Window.partitionBy("user_id") \
                            .orderBy(F.desc("count"))
    favorite_category_df = user_category_df.withColumn("rn",F.row_number().over(category_window)) \
                                           .filter(F.col("rn") == 1) \
                                           .select("user_id",F.col("category").alias("favorite_category"))
    
    user_device_df = df.withColumn("device",F.coalesce(F.col("device"), F.lit("unknown"))).groupBy("user_id", "device").count()
    device_window = Window.partitionBy("user_id") \
                          .orderBy(F.desc("count"))
    favorite_device_df = user_device_df.withColumn("rn",F.row_number().over(device_window)) \
                                       .filter(F.col("rn") == 1) \
                                       .select("user_id",F.col("device").alias("favorite_device"))
    
    user_profile_df = user_metrics_df.join(favorite_category_df,"user_id","left") \
                                     .join(favorite_device_df,"user_id","left") \
                                     .withColumn("updated_at",F.current_timestamp())


    parquet_partition_to_iceberg(
        user_profile_df,
        "user_profile",
        """
        CREATE TABLE IF NOT EXISTS demo.gold.user_profile (
            user_id STRING,

            total_view BIGINT,
            total_click BIGINT,
            total_add_to_cart BIGINT,
            total_purchase BIGINT,

            total_revenue DOUBLE,

            favorite_category STRING,
            favorite_device STRING,

            last_active_time TIMESTAMP,

            updated_at TIMESTAMP
        )
        USING iceberg
        """
    )


    spark.sql(f"""
    UPDATE demo.meta.pipeline_partitions
    SET processed = true,
        updated_at = current_timestamp()
    WHERE pipeline_name = '{PIPELINE_NAME}'
      AND start_time = '{start_time}'
      AND end_time = '{end_time}'
    """)
    
    print(f"Finished gold_to_iceberg partition start_time={start_time}, end_time = {end_time}")

spark.stop()