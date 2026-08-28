import streamlit as st
import os

st.set_page_config(page_title="Météo & Climat Analytics", layout="wide")

st.title("🌦️ Plateforme Analytics Météo & Climat")
st.subheader("Visualisation de la Couche Gold (Architecture Medallion)")

st.sidebar.header("Navigation")
st.sidebar.info("Données synchronisées avec Apache Airflow & Spark Gold")

# Dossier contenant tes graphiques
reports_dir = "reports"

col1, col2 = st.columns(2)

with col1:
    st.image(os.path.join(reports_dir, "1_temp_vs_historique.png"), caption="1. Température vs Normale Historique", use_container_width=True)
    st.image(os.path.join(reports_dir, "3_precipitations.png"), caption="3. Précipitations Mensuelles", use_container_width=True)
    st.image(os.path.join(reports_dir, "5_distribution_temperatures.png"), caption="5. Distribution Annuelle (Boxplot)", use_container_width=True)

with col2:
    st.image(os.path.join(reports_dir, "2_anomalies_thermiques.png"), caption="2. Anomalies Thermiques", use_container_width=True)
    st.image(os.path.join(reports_dir, "4_matrice_correlation.png"), caption="4. Matrice de Corrélation", use_container_width=True)
    st.image(os.path.join(reports_dir, "6_alertes_meteo.png"), caption="6. Nombre d'Alertes Météo", use_container_width=True)