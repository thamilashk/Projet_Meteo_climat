import os
os.environ["HADOOP_USER_NAME"] = "root"

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round as spark_round, min as spark_min, max as spark_max

sns.set_theme(style="darkgrid")

# 1. Initialisation Spark
spark = (
    SparkSession.builder.appName("MeteoGeneratePlots")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("--- GÉNÉRATION DES GRAPHIQUES GOLD (FORMAT AGRÉGÉ) ---")

output_dir = "/app/reports"
os.makedirs(output_dir, exist_ok=True)

# 2. Lecture et agrégation mensuelle directe via Spark
df_daily = spark.read.parquet("hdfs://namenode:9000/gold/meteo_daily_agg")

df_monthly = (
    df_daily.groupBy("year_month")
    .agg(
        spark_round(avg("avg_temperature"), 2).alias("avg_temperature"),
        spark_round(avg("hist_avg_temp_month"), 2).alias("hist_avg_temp_month"),
        spark_round(avg("temp_anomaly"), 2).alias("temp_anomaly"),
        spark_round(avg("total_precipitation"), 2).alias("total_precipitation")
    )
    .orderBy("year_month")
)

pdf_monthly = df_monthly.toPandas()

# ---------------------------------------------------------
# Graphique 1 : Température Mensuelle Réelle vs Historique
# ---------------------------------------------------------
plt.figure(figsize=(12, 5))
plt.plot(pdf_monthly["year_month"], pdf_monthly["avg_temperature"], label="Température Moyenne (°C)", color="#d9534f", linewidth=2, marker="o")
plt.plot(pdf_monthly["year_month"], pdf_monthly["hist_avg_temp_month"], label="Normale Mensuelle 2023 (°C)", color="#0275d8", linestyle="--", linewidth=2)

plt.title("1. Évolution Mensuelle : Température Réelle vs Historique", fontsize=13, fontweight="bold")
plt.xlabel("Année-Mois")
plt.ylabel("Température (°C)")
plt.xticks(rotation=45, ha="right")
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "1_temp_vs_historique.png"))
plt.close()

# ---------------------------------------------------------
# Graphique 2 : Anomalies Thermiques Mensuelles
# ---------------------------------------------------------
plt.figure(figsize=(12, 5))
colors = ["#d9534f" if x >= 0 else "#0275d8" for x in pdf_monthly["temp_anomaly"]]

plt.bar(pdf_monthly["year_month"], pdf_monthly["temp_anomaly"], color=colors, width=0.6)
plt.axhline(0, color="black", linestyle="-", linewidth=0.8)

plt.title("2. Anomalies Thermiques Mensuelles (Écart vs Historique)", fontsize=13, fontweight="bold")
plt.xlabel("Année-Mois")
plt.ylabel("Écart (°C)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "2_anomalies_thermiques.png"))
plt.close()

# ---------------------------------------------------------
# Graphique 3 : Précipitations Mensuelles
# ---------------------------------------------------------
plt.figure(figsize=(12, 5))
plt.bar(pdf_monthly["year_month"], pdf_monthly["total_precipitation"], color="#5bc0de", width=0.6)

plt.title("3. Précipitations Mensuelles (mm)", fontsize=13, fontweight="bold")
plt.xlabel("Année-Mois")
plt.ylabel("Précipitations (mm)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "3_precipitations.png"))
plt.close()

print("[OK] Tous les graphiques ont été régénérés proprement !")