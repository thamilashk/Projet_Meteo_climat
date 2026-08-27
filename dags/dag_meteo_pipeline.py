from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Configuration par défaut du DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="meteo_data_pipeline",
    default_args=default_args,
    description="Pipeline ETL Météo Climat (Silver -> Gold -> Reports)",
    schedule_interval="0 2 * * *",  # S'exécute chaque jour à 02h00 du matin
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["meteo", "spark", "hdfs"],
) as dag:

    # 1. Traitement Silver Historique
    task_silver_hist = BashOperator(
        task_id="spark_silver_historical",
        bash_command="docker exec -u root spark-master /opt/spark/bin/spark-submit /app/scripts/spark_batch_silver_historical.py",
    )

    # 2. Traitement Silver Temps Réel
    task_silver_rt = BashOperator(
        task_id="spark_silver_realtime",
        bash_command="docker exec -u root spark-master /opt/spark/bin/spark-submit /app/scripts/spark_batch_silver.py",
    )

    # 3. Traitement Gold (Agrégations & Anomalies)
    task_gold = BashOperator(
        task_id="spark_gold_processing",
        bash_command="docker exec -u root spark-master /opt/spark/bin/spark-submit /app/scripts/spark_batch_gold.py",
    )

    # 4. Génération des graphiques (Rapports)
    task_plots = BashOperator(
        task_id="generate_gold_plots",
        bash_command="docker exec -u root spark-master /opt/spark/bin/spark-submit /app/scripts/generate_plots.py",
    )

    # ---------------------------------------------------------
    # DÉFINITION DES DÉPENDANCES
    # ---------------------------------------------------------
    # Les 2 Silver s'exécutent en parallèle, puis Gold, puis les graphiques
    [task_silver_hist, task_silver_rt] >> task_gold >> task_plots