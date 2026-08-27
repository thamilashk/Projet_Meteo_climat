import os

os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp

# 1. SparkSession
spark = (
    SparkSession.builder.appName("MeteoBronzeToSilverHistoricalBatch")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("--- TRAITEMENT SILVER : HISTORIQUE 2023 (BRONZE -> SILVER) ---")

input_path = "hdfs://namenode:9000/bronze/source=meteo/year=2023/meteo_paris_2023.csv"

# 2. Lecture brute puis filtrage des lignes d'en-tête Open-Meteo
raw_df = spark.read.text(input_path)

# On filtre la ligne d'en-tête Open-Meteo initiale et la ligne vide
filtered_lines = raw_df.filter(
    ~col("value").startswith("latitude")
    & ~col("value").startswith("48.")
    & (col("value") != "")
)

# 3. Parsing du CSV nettoyé
df_csv = spark.read.option("header", "true").csv(filtered_lines.rdd.map(lambda r: r[0]))

# 4. Renommer et typer les colonnes pour matcher le schéma Silver
silver_hist_df = (
    df_csv
    .withColumnRenamed("time", "timestamp")
    .withColumnRenamed("temperature_2m (°C)", "temperature")
    .withColumnRenamed("relative_humidity_2m (%)", "humidity")
    .withColumnRenamed("precipitation (mm)", "precipitation")
    .withColumn("station_id", lit("PARIS_2023"))
    .withColumn("latitude", lit(48.89279))
    .withColumn("longitude", lit(2.2920206))
    .withColumn("temperature", col("temperature").cast("double"))
    .withColumn("humidity", col("humidity").cast("double"))
    .withColumn("precipitation", col("precipitation").cast("double"))
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .withColumn("year", lit(2023))
    .dropDuplicates(["station_id", "timestamp"])
)

# 5. Écriture dans HDFS Silver
output_path = "hdfs://namenode:9000/silver/meteo_historical"

silver_hist_df.write.mode("overwrite").partitionBy("year").parquet(output_path)

print(f"[OK] Table Silver historique générée avec succès : {output_path}")