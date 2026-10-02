from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, explode, from_json, count, avg, current_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType, BooleanType, IntegerType
from delta.tables import DeltaTable

#.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2")\
# .config(
#         "spark.jars.packages",
#         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,"
#         "org.apache.kafka:kafka-clients:3.5.2,"
#         "org.apache.commons:commons-pool2:2.11.1,"
#         "io.delta:delta-spark_2.12:3.2.0,"
#         "org.apache.hadoop:hadoop-aws:3.3.4,"
#         "com.amazonaws:aws-java-sdk-bundle:1.12.262"
#     )
spark = SparkSession.builder\
    .appName("DebeziumCDCpipeline")\
    .master("spark://spark-master:7077")\
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,"
        "org.apache.kafka:kafka-clients:3.5.2,"
        "org.apache.commons:commons-pool2:2.11.1,"
        "io.delta:delta-spark_2.12:3.2.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

hconf = spark.sparkContext._jsc.hadoopConfiguration()
hconf.set("fs.s3a.endpoint", "http://minio:9000")
hconf.set("fs.s3a.access.key", "matrix")
hconf.set("fs.s3a.secret.key", "matrix123")
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.connection.ssl.enabled", "false")
hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


# 2. Göndərdiyin JSON-a uyğun exact Sxem (Schema) təyin edirik
user_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True)
])

source_schema = StructType([
    StructField("db", StringType(), True),
    StructField("table", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

debezium_schema = StructType([
    StructField("before", user_schema, True),
    StructField("after", user_schema, True),
    StructField("source", source_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

# 3. Kafka-dan məlumatı oxuyuruq
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092") \
    .option("subscribe", "cdc.public.my_test") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), debezium_schema).alias("data"))

# query = parsed_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

def upsert_to_delta(microBatchDF, batch_id):
    cleaned_batch = microBatchDF.select(
        expr("CASE WHEN data.op = 'd' THEN data.before.id ELSE data.after.id END").alias("id"),
        col("data.after.name").alias("name"),
        col("data.after.created_at").alias("created_at"),
        col("data.after.updated_at").alias("updated_at"),
        col("data.op").alias("op")
    ).filter("id IS NOT NULL")

    # 1. Hər batch daxilində deduplication edirik
    # 2. MinIO-dakı Delta cədvəli ilə MERGE (Upsert) edirik
    if DeltaTable.isDeltaTable(spark, "s3a://my-bucket/users"):
        deltaTable = DeltaTable.forPath(spark, "s3a://my-bucket/users")
        deltaTable.alias("target").merge(cleaned_batch.alias("source"),"target.id = source.id")\
            .whenMatchedDelete(condition = "source.op = 'd'") \
            .whenMatchedUpdateAll(condition = "source.op != 'd'") \
            .whenNotMatchedInsertAll(condition = "source.op != 'd'") \
            .execute()
    else:
        microBatchDF.write.format("delta").mode("append").save("s3a://my-bucket/users")

# Streaming-i başladırıq:
query = parsed_df.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "s3a://my-bucket/checkpoints") \
    .start()
# butun axınılarin daimi açıq qalması üçün gözləyirik
query.awaitTermination()

