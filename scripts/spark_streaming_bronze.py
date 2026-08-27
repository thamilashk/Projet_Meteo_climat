import os

os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format

# 1. Configuration pour exécution au sein du réseau Docker
spark = (
    SparkSession.builder.appName("MeteoRealtimeBronzeIngestion")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("--- DÉMARRAGE DU STREAMING BRONZE SUR SPARK MASTER (KAFKA -> HDFS) ---")

# 2. Lecture du flux Kafka (adresse interne Docker)
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "meteo-realtime")
    .option("startingOffsets", "latest")
    .load()
)

# 3. Extraction et horodatage
bronze_stream_df = (
    kafka_df.selectExpr("CAST(value AS STRING) as raw_json")
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn(
        "ingestion_date", date_format(col("ingestion_timestamp"), "yyyy-MM-dd")
    )
)

# 4. Écriture dans HDFS
checkpoint_path = "hdfs://namenode:9000/bronze/_checkpoints/meteo_realtime"
output_path = "hdfs://namenode:9000/bronze/source=meteo_realtime"

query = (
    bronze_stream_df.writeStream.format("parquet")
    .option("checkpointLocation", checkpoint_path)
    .option("path", output_path)
    .partitionBy("ingestion_date")
    .outputMode("append")
    .start()
)

print(f"[OK] Ingestion Bronze en cours vers : {output_path}")
query.awaitTermination()