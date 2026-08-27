import os

os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import DoubleType, StringType, StructType

# 1. SparkSession
spark = (
    SparkSession.builder.appName("MeteoBronzeToSilverBatch")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("--- TRAITEMENT SILVER : TEMPS RÉEL (BRONZE -> SILVER) ---")

# 2. Schéma des données JSON
schema = (
    StructType()
    .add("station_id", StringType())
    .add("latitude", DoubleType())
    .add("longitude", DoubleType())
    .add("temperature", DoubleType())
    .add("humidity", DoubleType())
    .add("pressure", DoubleType())
    .add("timestamp", StringType())
)

# 3. Lecture du dossier Bronze temps réel
bronze_realtime_df = spark.read.parquet("hdfs://namenode:9000/bronze/source=meteo_realtime")

# 4. Extraction et nettoyage
silver_realtime_df = (
    bronze_realtime_df
    .withColumn("parsed_data", from_json(col("raw_json"), schema))
    .select("parsed_data.*", "ingestion_timestamp", "ingestion_date")
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .dropDuplicates(["station_id", "timestamp"])
)

# 5. Écriture dans la couche Silver HDFS
output_path = "hdfs://namenode:9000/silver/meteo_realtime"

silver_realtime_df.write.mode("overwrite").partitionBy("ingestion_date").parquet(output_path)

print(f"[OK] Table Silver temps réel générée avec succès : {output_path}")