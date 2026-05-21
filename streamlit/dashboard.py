import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
import os
import time

st.set_page_config(page_title="NYC Taxi Dashboard", page_icon="🚕", layout="wide")

def get_db():
    return mysql.connector.connect(
        host=os.environ.get("MYSQL_HOST",     "mysql-gold"),
        user=os.environ.get("MYSQL_USER",     "taxi_user"),
        password=os.environ.get("MYSQL_PASSWORD", "taxi1234"),
        database=os.environ.get("MYSQL_DATABASE", "gold")
    )

def load_data(query):
    for _ in range(5):
        try:
            return pd.read_sql(query, get_db())
        except Exception:
            time.sleep(3)
    return pd.DataFrame()

# ── Header ────────────────────────────────────────────────────
st.title("🚕 NYC Taxi Analytics Dashboard")
st.markdown("**Architecture Lakehouse Médaillon** — Raw (HDFS) → Silver (Hive) → Gold (MySQL)")
st.divider()

# ── Chargement des données Gold ───────────────────────────────
df_zones    = load_data("SELECT * FROM kpi_par_zone ORDER BY ca_total DESC")
df_heures   = load_data("SELECT * FROM kpi_par_heure ORDER BY heure")
df_paiement = load_data("SELECT * FROM kpi_paiement")
df_aeroport = load_data("SELECT * FROM kpi_aeroports")

# ── KPIs globaux ──────────────────────────────────────────────
if not df_zones.empty:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total trajets",       "{:,}".format(int(df_zones["nb_trajets"].sum())))
    c2.metric("CA total ($)",        "{:,.0f}".format(df_zones["ca_total"].sum()))
    c3.metric("Tarif moyen ($)",     "{:.2f}".format(df_zones["tarif_moyen"].mean()))
    c4.metric("Pourboire moyen ($)", "{:.2f}".format(df_zones["pourboire_moyen"].mean()))

st.divider()

# ── Top 15 zones rentables ────────────────────────────────────
st.subheader("Top 15 zones les plus rentables")
if not df_zones.empty:
    fig1 = px.bar(df_zones.head(15), x="ca_total", y="zone",
                  color="borough", orientation="h",
                  labels={"ca_total": "CA ($)", "zone": "Zone"})
    fig1.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig1, use_container_width=True)

st.divider()

# ── Activité par heure ────────────────────────────────────────
st.subheader("Activité par heure de la journée")
if not df_heures.empty:
    fig2 = px.line(
        df_heures.groupby("heure")["nb_trajets"].sum().reset_index(),
        x="heure", y="nb_trajets", markers=True,
        labels={"heure": "Heure", "nb_trajets": "Nb trajets"}
    )
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── Modes de paiement ─────────────────────────────────────────
st.subheader("Modes de paiement par borough")
if not df_paiement.empty:
    fig3 = px.bar(df_paiement, x="borough", y="nb_trajets",
                  color="mode_paiement", barmode="group")
    st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── Aéroports ────────────────────────────────────────────────
st.subheader("Statistiques trajets aéroports")
if not df_aeroport.empty:
    c1, c2 = st.columns(2)
    with c1:
        fig4a = px.bar(df_aeroport, x="aeroport", y="nb_trajets", color="aeroport",
                       title="Nombre de trajets")
        st.plotly_chart(fig4a, use_container_width=True)
    with c2:
        fig4b = px.bar(df_aeroport, x="aeroport", y="tarif_moyen", color="aeroport",
                       title="Tarif moyen ($)")
        st.plotly_chart(fig4b, use_container_width=True)

st.caption("Source : NYC Taxi & Limousine Commission (TLC) — Open Data 2024")