import os

os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    date_format,
    max,
    min,
    round,
    sum as spark_sum,
    to_date,
    to_timestamp,
    when,
)

# 1. Initialisation Spark
spark = (
    SparkSession.builder.appName("MeteoSilverToGoldBatch")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("--- DÉMARRAGE DU TRAITEMENT GOLD ---")

# 2. Lecture des tables Silver
silver_realtime = spark.read.parquet("hdfs://namenode:9000/silver/meteo_realtime")
silver_historical = spark.read.parquet("hdfs://namenode:9000/silver/meteo_historical")

# Gestion des précipitations
if "precipitation" not in silver_realtime.columns:
    silver_realtime = silver_realtime.withColumn("precipitation", col("humidity") * 0)

# Correction du timestamp temps réel (Fallback sur ingestion_timestamp si timestamp est NULL)
df_rt = silver_realtime.withColumn(
    "clean_timestamp",
    coalesce(to_timestamp(col("timestamp")), to_timestamp(col("ingestion_timestamp")), to_timestamp(col("ingestion_date")))
).select(
    "station_id",
    col("clean_timestamp").alias("timestamp"),
    "temperature",
    "humidity",
    "precipitation"
)

df_hist = silver_historical.select(
    "station_id",
    to_timestamp(col("timestamp")).alias("timestamp"),
    "temperature",
    "humidity",
    "precipitation"
)

# 3. Union des données
full_df = df_rt.unionByName(df_hist).filter(col("timestamp").isNotNull()).withColumn("date", to_date("timestamp"))

# 4. Moyenne historique mensuelle de référence
monthly_reference = (
    silver_historical.withColumn("month", date_format("timestamp", "MM"))
    .groupBy("month")
    .agg(round(avg("temperature"), 2).alias("hist_avg_temp_month"))
)

# 5. Table Gold 1 : Agrégations quotidiennes + Anomalies
gold_daily = (
    full_df.withColumn("month", date_format("date", "MM"))
    .groupBy("station_id", "date", "month")
    .agg(
        round(avg("temperature"), 2).alias("avg_temperature"),
        round(min("temperature"), 2).alias("min_temperature"),
        round(max("temperature"), 2).alias("max_temperature"),
        round(avg("humidity"), 2).alias("avg_humidity"),
        round(spark_sum("precipitation"), 2).alias("total_precipitation"),
        count("timestamp").alias("record_count"),
    )
    .join(monthly_reference, on="month", how="left")
    .withColumn(
        "temp_anomaly",
        round(col("avg_temperature") - col("hist_avg_temp_month"), 2),
    )
    .withColumn("year_month", date_format("date", "yyyy-MM"))
    .drop("month")
)

# 6. Table Gold 2 : Alertes Météo
gold_alerts = (
    full_df.withColumn(
        "alert_gel", when(col("temperature") < 0, 1).otherwise(0)
    )
    .withColumn(
        "alert_canicule", when(col("temperature") > 30, 1).otherwise(0)
    )
    .withColumn(
        "alert_pluie_forte", when(col("precipitation") > 10, 1).otherwise(0)
    )
    .filter(
        (col("alert_gel") == 1)
        | (col("alert_canicule") == 1)
        | (col("alert_pluie_forte") == 1)
    )
    .withColumn("year_month", date_format("date", "yyyy-MM"))
)

# 7. Écriture dans HDFS
output_daily = "hdfs://namenode:9000/gold/meteo_daily_agg"
output_alerts = "hdfs://namenode:9000/gold/meteo_alerts"

gold_daily.write.mode("overwrite").partitionBy("year_month").parquet(output_daily)
gold_alerts.write.mode("overwrite").partitionBy("year_month").parquet(output_alerts)

print(f"[OK] Table Gold 1 mise à jour : {output_daily}")
print(f"[OK] Table Gold 2 mise à jour : {output_alerts}")