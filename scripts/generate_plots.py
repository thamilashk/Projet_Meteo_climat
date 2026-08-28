import os
os.environ["HADOOP_USER_NAME"] = "root"

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum as spark_sum, round as spark_round, date_format

sns.set_theme(style="darkgrid")

# 1. Initialisation Spark
spark = (
    SparkSession.builder.appName("MeteoGeneratePlots")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("--- GÉNÉRATION DES GRAPHIQUES GOLD (6 INSIGHTS COMPLET) ---")

output_dir = "/app/reports"
os.makedirs(output_dir, exist_ok=True)

# 2. Lecture Gold
df_daily = spark.read.parquet("hdfs://namenode:9000/gold/meteo_daily_agg")

df_monthly = (
    df_daily.groupBy("year_month")
    .agg(
        spark_round(avg("avg_temperature"), 2).alias("avg_temperature"),
        spark_round(avg("hist_avg_temp_month"), 2).alias("hist_avg_temp_month"),
        spark_round(avg("temp_anomaly"), 2).alias("temp_anomaly"),
        spark_round(spark_sum("total_precipitation"), 2).alias("total_precipitation")
    )
    .orderBy("year_month")
)

pdf_monthly = df_monthly.toPandas()

# Graphique 1 : Température Réelle vs Normale
plt.figure(figsize=(12, 5))
plt.plot(pdf_monthly["year_month"], pdf_monthly["avg_temperature"], label="Température Moyenne (°C)", color="#d9534f", linewidth=2, marker="o")
plt.plot(pdf_monthly["year_month"], pdf_monthly["hist_avg_temp_month"], label="Normale Climatique (2023-2025)", color="#0275d8", linestyle="--", linewidth=2)
plt.title("1. Évolution Mensuelle : Température Réelle vs Normale Historique", fontsize=12, fontweight="bold")
plt.xlabel("Année-Mois")
plt.ylabel("Température (°C)")
plt.xticks(rotation=45, ha="right")
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "1_temp_vs_historique.png"))
plt.close()

# Graphique 2 : Anomalies Thermiques
plt.figure(figsize=(12, 5))
colors = ["#d9534f" if x >= 0 else "#0275d8" for x in pdf_monthly["temp_anomaly"]]
plt.bar(pdf_monthly["year_month"], pdf_monthly["temp_anomaly"], color=colors, width=0.6)
plt.axhline(0, color="black", linestyle="-", linewidth=0.8)
plt.title("2. Anomalies Thermiques Mensuelles (Écart vs Normale 2023-2025)", fontsize=12, fontweight="bold")
plt.xlabel("Année-Mois")
plt.ylabel("Écart (°C)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "2_anomalies_thermiques.png"))
plt.close()

# Graphique 3 : Précipitations
plt.figure(figsize=(12, 5))
plt.bar(pdf_monthly["year_month"], pdf_monthly["total_precipitation"], color="#5bc0de", width=0.6)
plt.title("3. Cumul Mensuel des Précipitations (mm)", fontsize=12, fontweight="bold")
plt.xlabel("Année-Mois")
plt.ylabel("Précipitations (mm)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "3_precipitations.png"))
plt.close()

# Graphique 4 : Matrice de Corrélation
plt.figure(figsize=(7, 5))
corr_matrix = pdf_monthly[["avg_temperature", "temp_anomaly", "total_precipitation"]].corr()
sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", vmin=-1, vmax=1, fmt=".2f")
plt.title("4. Corrélation Température / Anomalies / Précipitations", fontsize=11, fontweight="bold")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "4_matrice_correlation.png"))
plt.close()

# Graphique 5 : Distribution des Températures par Année (Boxplot)
pdf_daily_year = df_daily.withColumn("year", date_format("date", "yyyy")).select("year", "avg_temperature").toPandas()
plt.figure(figsize=(9, 5))
sns.boxplot(data=pdf_daily_year, x="year", y="avg_temperature", palette="Set2")
plt.title("5. Distribution des Températures Journalières par Année", fontsize=12, fontweight="bold")
plt.xlabel("Année")
plt.ylabel("Température Moyenne (°C)")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "5_distribution_temperatures.png"))
plt.close()

# Graphique 6 : Alertes Météo par Année
try:
    df_alerts = spark.read.parquet("hdfs://namenode:9000/gold/meteo_alerts")
    df_alerts_summary = (
        df_alerts.withColumn("year", date_format("date", "yyyy"))
        .groupBy("year")
        .agg(
            spark_sum("alert_gel").alias("Gel (<0°C)"),
            spark_sum("alert_canicule").alias("Canicule (>30°C)"),
            spark_sum("alert_pluie_forte").alias("Pluie Forte (>10mm)")
        )
        .orderBy("year")
    )
    pdf_alerts = df_alerts_summary.toPandas().set_index("year")
    fig, ax = plt.subplots(figsize=(9, 5))
    pdf_alerts.plot(kind="bar", ax=ax, colormap="Accent", width=0.6)
    plt.title("6. Nombre de Jours d'Alertes Météo par Année", fontsize=12, fontweight="bold")
    plt.xlabel("Année")
    plt.ylabel("Nombre de Jours")
    plt.xticks(rotation=0)
    plt.legend(title="Type d'Alerte")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "6_alertes_meteo.png"))
    plt.close()
except Exception as e:
    print(f"Information alertes non générée : {e}")

print("[OK] Tous les 6 graphiques ont été générés dans /app/reports !")