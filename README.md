# Pipeline ETL & Analytics Météo Climat (Architecture Medallion & Streaming)

Ce projet met en œuvre une plateforme **Big Data complète** permettant l'ingestion en temps réel, le traitement par lots (batch) et l'analyse décisionnelle de données météorologiques historiques et temps réel pour la région parisienne.

L'architecture repose sur l'approche **Medallion Architecture (Bronze -> Silver -> Gold)**, le moteur de calcul **Apache Spark**, le broker de messages **Apache Kafka**, le système de fichiers distribué **HDFS**, et l'orchestrateur **Apache Airflow**.

---

## 🏗️ Architecture Globale du Projet

```
[ Open-Meteo API ]
       │
       ▼
[ kafka_producer.py ]
       │
       ▼ (Kafka Topic: meteo-realtime)
[ spark_streaming_bronze.py ] ──────┐
                                   │
[ Historic CSV (2023-2025) ] ─────┼──► [ HDFS /bronze ]
                                   │
                                   ▼
                       [ spark_batch_silver*.py ]
                                   │
                                   ▼
                            [ HDFS /silver ]
                                   │
                                   ▼
                       [ spark_batch_gold.py ]
                                   │
                                   ▼
                            [ HDFS /gold ]
                                   │
                                   ▼
                        [ generate_plots.py ]
                                   │
                                   ▼
                        [ Reports / PNGs ]
```

---

## 🛠️ Stack Technique

- **Ingestion & Streaming :** Python `requests`, Apache Kafka, Zookeeper, Spark Structured Streaming
- **Stockage Distributed :** Hadoop HDFS (DataNode & NameNode)
- **Traitement & Analytics :** PySpark (Spark Master & Workers, formats Parquet)
- **Orchestration :** Apache Airflow (Scheduler, Webserver, Postgres Metastore)
- **Visualisation :** Matplotlib, Seaborn, Pandas
- **Conteneurisation :** Docker & Docker Compose

---

## 📂 Structure du Répertoire

```text
.
├── dags/
│   └── dag_meteo_pipeline.py      # DAG Airflow (Orchestration Silver -> Gold -> Plots)
├── scripts/
│   ├── kafka_producer.py          # Producteur Kafka (Appel API Open-Meteo)
│   ├── spark_streaming_bronze.py  # Ingestion Streaming Kafka vers HDFS Bronze
│   ├── spark_batch_silver_historical.py # Nettoyage des CSV historiques vers Silver
│   ├── spark_batch_silver.py      # Structuration du streaming temps réel vers Silver
│   ├── spark_batch_gold.py        # Consolidation, agrégations & calcul d'anomalies
│   └── generate_plots.py          # Génération des graphiques analytiques
├── reports/                       # Dossier de sortie des graphiques PNG
├── docker-compose.yml             # Déploiement multi-conteneurs
└── README.md                      # Documentation principale
```

---

## 🚀 Guide de Démarrage Rapide

### 1. Lancement de la Stack Docker
```bash
docker-compose up -d
sudo chmod 666 /var/run/docker.sock
```

### 2. Installation des Dépendances Python sur Spark
```bash
docker exec -u root spark-master pip install matplotlib pandas seaborn
```

### 3. Démarrage de l'Ingestion Streaming (2 Terminaux)

**Terminal 1 — Producteur Kafka :**
```bash
python3 scripts/kafka_producer.py
```

**Terminal 2 — Consumer Spark Streaming :**
```bash
docker exec -u root -it spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /app/scripts/spark_streaming_bronze.py
```

### 4. Lancement de l'Orchestration Airflow
1. Accéder à l'interface Airflow sur `http://localhost:8082` (Identifiants: `airflow` / `airflow`).
2. Activer le DAG **`meteo_data_pipeline`**.
3. Cliquer sur **Trigger DAG** (`▶`).

---

## 📊 Données & Couches Medallion

- **Bronze (`/bronze`) :** Données brutes JSON (temps réel) et CSV historiques Open-Meteo.
- **Silver (`/silver`) :** Données nettoyées, typées, dédoublonnées et converties au format Parquet.
- **Gold (`/gold`) :** Tables d'agrégation quotidienne, calcul des normales climatiques mensuelles (référence 2023), calcul des anomalies thermiques (`avg_temp - normal_temp`) et génération des alertes météo (gel, canicule, forte pluie).
