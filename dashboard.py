import streamlit as st
import os

# Configuration de la page Streamlit
st.set_page_config(
    page_title="Dashboard Météo & Climat - Layer Gold",
    page_icon="🌦️",
    layout="wide"
)

# Dossier des rapports
reports_dir = "reports"

# --- SIDEBAR (NAVIGATION ET COMMANDES) ---
st.sidebar.image("https://img.icons8.com/color/96/weather.png", width=70)
st.sidebar.title("Météo Analytics")
st.sidebar.markdown("**Architecture Medallion (HDFS / Spark / Airflow)**")
st.sidebar.divider()

if st.sidebar.button("🔄 Actualiser les données", use_container_width=True):
    st.rerun()

st.sidebar.info("""
**Pipeline Status:**
- **Bronze / Silver:** Live HDFS
- **Gold Aggregation:** Spark Batch
- **Orchestration:** Airflow DAG
""")

# --- HEADER ET KPIS DYNAMIQUES ---
st.title("🌦️ Plateforme Analytics Météo & Climat")
st.markdown("##### Visualisation haute définition de la couche **Gold** issue du traitement distribué Spark")

# Ligne de Métriques / KPIs
col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)
col_kpi1.metric(label="Période Analysée", value="2023 - 2026")
col_kpi2.metric(label="Fréquence Moteur", value="Micro-batch 30s")
col_kpi3.metric(label="Moteur Big Data", value="Apache Spark 3.5")
col_kpi4.metric(label="Stockage Distribué", value="HDFS Parquet")

st.divider()

# --- DYNAMISME AVEC DES ONGLETS (TABS) ---
tab1, tab2, tab3 = st.tabs([
    "📈 1. Températures & Anomalies", 
    "🌧️ 2. Précipitations & Risques Météo", 
    "📊 3. Corrélation & Distribution"
])

# ==========================================
# ONGLET 1 : TEMPÉRATURES & ANOMALIES
# ==========================================
with tab1:
    st.header("1. Température Réelle vs Normale Historique")
    path_img1 = os.path.join(reports_dir, "1_temp_vs_historique.png")
    if os.path.exists(path_img1):
        st.image(path_img1, use_container_width=True)
        with st.expander("🔍 Interprétation de l'insight (Click to expand)"):
            st.write("""
            Ce graphique compare la température moyenne mesurée chaque mois avec la normale climatique calculée sur l'historique 2023-2025. 
            Il permet de repérer immédiatement si les températures d'un mois s'écartent significativement des moyennes saisonnières.
            """)
    else:
        st.warning("Image 1_temp_vs_historique.png non disponible.")

    st.divider()

    st.header("2. Anomalies Thermiques Mensuelles")
    path_img2 = os.path.join(reports_dir, "2_anomalies_thermiques.png")
    if os.path.exists(path_img2):
        st.image(path_img2, use_container_width=True)
        with st.expander("🔍 Interprétation de l'insight"):
            st.write("""
            Représentation sous forme d'écart à la normale (en °C). 
            Les barres rouges désignent un réchauffement anormal par rapport à la référence, tandis que les barres bleues désignent des températures plus fraîches que la moyenne.
            """)
    else:
        st.warning("Image 2_anomalies_thermiques.png non disponible.")

# ==========================================
# ONGLET 2 : PRÉCIPITATIONS & RISQUES
# ==========================================
with tab2:
    st.header("3. Cumul Mensuel des Précipitations")
    path_img3 = os.path.join(reports_dir, "3_precipitations.png")
    if os.path.exists(path_img3):
        st.image(path_img3, use_container_width=True)
        with st.expander("🔍 Interprétation de l'insight"):
            st.write("""
            Cumul volumétrique des précipitations en millimètres (mm). Permet d'identifier les périodes de sécheresse ou au contraire les mois hyper-arrosés.
            """)
    else:
        st.warning("Image 3_precipitations.png non disponible.")

    st.divider()

    st.header("6. Détection et Comptage des Alertes Météo")
    path_img6 = os.path.join(reports_dir, "6_alertes_meteo.png")
    if os.path.exists(path_img6):
        st.image(path_img6, use_container_width=True)
        with st.expander("🔍 Interprétation de l'insight"):
            st.write("""
            Bilan annuel des journées d'alertes calculées dynamiquement par Spark Gold :
            - **Gel :** Température minimale < 0°C
            - **Canicule :** Température maximale > 30°C
            - **Pluie Forte :** Cumul quotidien > 10 mm
            """)
    else:
        st.warning("Image 6_alertes_meteo.png non disponible.")

# ==========================================
# ONGLET 3 : CORRÉLATION & DISTRIBUTION
# ==========================================
with tab3:
    st.header("4. Matrice de Corrélation Multi-Variables")
    path_img4 = os.path.join(reports_dir, "4_matrice_correlation.png")
    if os.path.exists(path_img4):
        st.image(path_img4, use_container_width=True)
        with st.expander("🔍 Interprétation de l'insight"):
            st.write("""
            Carte thermique (Heatmap) montrant l'interaction entre la température, les précipitations et les anomalies thermiques.
            Un coefficient proche de +1 montre une relation directe forte, tandis qu'un coefficient proche de -1 montre une relation inverse.
            """)
    else:
        st.warning("Image 4_matrice_correlation.png non disponible.")

    st.divider()

    st.header("5. Distribution des Températures Journalières par Année")
    path_img5 = os.path.join(reports_dir, "5_distribution_temperatures.png")
    if os.path.exists(path_img5):
        st.image(path_img5, use_container_width=True)
        with st.expander("🔍 Interprétation de l'insight"):
            st.write("""
            Analyse de la dispersion (Boxplot) de la température quotidienne année par année.
            Permet de comparer la médiane, les percentiles (25%-75%) et de détecter les vagues de froid ou de chaleur extrêmes.
            """)
    else:
        st.warning("Image 5_distribution_temperatures.png non disponible.")