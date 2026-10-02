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

query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()



# 4. JSON Parse & Sütunlar (Düzgün id və name çıxarırıq)
# parsed_df = kafka_df \
#     .selectExpr("CAST(value AS STRING) as json_string") \
#     .select(from_json(col("json_string"), debezium_schema).alias("data")) \
#     .select(
#         col("data.op").alias("operation"),
#         # Əgər DELETE (d) olarsa 'before.id', əks halda 'after.id' götürülür
#         col("data.after.id").alias("after_id"),
#         col("data.after.name").alias("after_name"),
#         col("data.before.id").alias("before_id"),
#         # Birləşdirilmiş id və name sütunları yaradırıq ki, Delta-ya birbaşa yazıla bilsin:
#         expr("CASE WHEN data.op = 'd' THEN data.before.id ELSE data.after.id END").alias("id"),
#         expr("CASE WHEN data.op = 'd' THEN data.before.name ELSE data.after.name END").alias("name")
#     )



# # 5. Upsert (MERGE) Funksiyası
# def upsert_to_delta(microBatchDF, batch_id):
#     if microBatchDF.isEmpty():
#         return
    
#     delta_path = "s3a://my-bucket/users"
    
#     # Əgər cədvəl artıq Delta cədvəlidirsə, MERGE edirik
#     if DeltaTable.isDeltaTable(spark, delta_path):
#         deltaTable = DeltaTable.forPath(spark, delta_path)
        
#         deltaTable.alias("target").merge(
#             microBatchDF.alias("source"),
#             "target.id = source.id"
#         ) \
#         .whenMatchedDelete(condition="source.operation = 'd'") \
#         .whenMatchedUpdate(
#             condition="source.operation != 'd'",
#             set={"id": "source.id", "name": "source.name"}
#         ) \
#         .whenNotMatchedInsert(
#             condition="source.operation != 'd'",
#             values={"id": "source.id", "name": "source.name"}
#         ) \
#         .execute()
#     else:
#         # Cədvəl ilk dəfə yaranırsa: silinməmiş (c/u) sətirləri yazırıq
#         initial_df = microBatchDF.filter("operation != 'd'").select("id", "name")
#         initial_df.write.format("delta").mode("append").save(delta_path)

# # 6. Streaming-i başladırıq
# query = parsed_df.writeStream \
#     .foreachBatch(upsert_to_delta) \
#     .option("checkpointLocation", "s3a://my-bucket/checkpoints") \
#     .start()

# query.awaitTermination()










# # 4. Kafka-dan gələn binary 'value' dəyərini JSON-a və sütunlara çeviririk
# parsed_df = kafka_df \
#     .selectExpr("CAST(value AS STRING) as json_string") \
#     .select(from_json(col("json_string"), debezium_schema).alias("data")) \
#     .select(
#         col("data.op").alias("operation"),
#         col("data.source.ts_ms").alias("event_timestamp"),
#         # Əgər delete-dirsə 'before' içindəki ID-ni, yoxsa 'after' içindəki ID-ni götürürük
#         col("data.after.id").alias("after_id"),
#         col("data.after.name").alias("after_name"),
#         col("data.before.id").alias("before_id"),
#         col("data.before.name").alias("before_name")
#     )

# # 5. Konsola (terminala) çıxarırıq (Hər yeni məlumat gələndə cədvəl kimi göstərəcək)
# query = parsed_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()


# def upsert_to_delta(microBatchDF, batch_id):
#     # 1. Hər batch daxilində deduplication edirik
#     # 2. MinIO-dakı Delta cədvəli ilə MERGE (Upsert) edirik
#     if DeltaTable.isDeltaTable(spark, "s3a://my-bucket/users"):
#         deltaTable = DeltaTable.forPath(spark, "s3a://my-bucket/users")
        
#         deltaTable.alias("target").merge(
#             microBatchDF.alias("source"),
#             "target.id = source.id"
#         ).whenMatchedUpdateAll() \
#         .whenNotMatchedInsertAll() \
#         .execute()
#     else:
#         microBatchDF.write.format("delta").mode("append").save("s3a://my-bucket/users")

# # Streaming-i başladırıq:
# query = parsed_df.writeStream \
#     .foreachBatch(upsert_to_delta) \
#     .option("checkpointLocation", "s3a://my-bucket/checkpoints") \
#     .start()
# # butun axınılarin daimi açıq qalması üçün gözləyirik
# spark.streams.awaitAnyTermination()
# spark.stop()
