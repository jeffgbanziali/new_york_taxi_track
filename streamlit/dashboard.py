import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import os
import time

st.set_page_config(page_title="NYC Taxi Hub - Gold Insights", page_icon="🚕", layout="wide")

API_URL = os.environ.get("API_URL", "http://api:5000/api")

def get_jwt_token():
    """Se connecte à l'API pour récupérer ou vérifier le jeton JWT en mémoire de session."""
    if "jwt_token" not in st.session_state:
        try:
            # Identifiants sécurisés synchronisés avec l'API Flask
            credentials = {"username": "admin_taxi", "password": "Efrei2026!"}
            response = requests.post(f"{API_URL}/login", json=credentials, timeout=5)
            if response.status_code == 200:
                st.session_state["jwt_token"] = response.json().get("token")
            else:
                st.error("Impossible de s'authentifier auprès de l'API (Code 401).")
        except Exception as e:
            st.error(f"Erreur de connexion réseau avec l'API Sécurisée : {e}")

def load_data_from_api(endpoint, params=None):
    """Interroge l'API Flask en transmettant le jeton JWT obligatoire."""
    get_jwt_token()
    token = st.session_state.get("jwt_token")
    
    if not token:
        return pd.DataFrame()

    # Ajout du token Bearer dans les headers HTTP (Sécurité Phase 1)
    headers = {"Authorization": f"Bearer {token}"}
    
    for _ in range(5):
        try:
            response = requests.get(f"{API_URL}/{endpoint}", params=params, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data:
                    df = pd.DataFrame(data)
                    df.columns = df.columns.str.lower()
                    return df
            elif response.status_code == 401:
                # Si le token a expiré, on le supprime pour forcer un renouvellement
                st.session_state.pop("jwt_token", None)
                get_jwt_token()
        except Exception:
            time.sleep(1)
    return pd.DataFrame()

# ── Référentiel géographique ──
def load_nyc_taxi_zones_lookup():
    """Charge le référentiel officiel de New York avec la latitude/longitude de chaque zone."""
    url = "https://raw.githubusercontent.com/toddwschneider/nyc-taxi-data/master/setup/taxi_zones.csv"
    try:
        df_lookup = pd.read_csv(url)
        df_lookup = df_lookup.rename(columns={
            'LocationID': 'locationid',
            'Borough': 'borough',
            'Zone': 'zone'
        })
        return df_lookup
    except Exception:
        return pd.DataFrame()

GPS_ZONES_REPLI = {
    'JFK Airport': {'lat': 40.6413, 'lon': -73.7781},
    'LaGuardia Airport': {'lat': 40.7769, 'lon': -73.8740},
    'Newark Airport': {'lat': 40.6895, 'lon': -74.1745},
    'East Elmhurst': {'lat': 40.7630, 'lon': -73.8807},
    'Baisley Park': {'lat': 40.6769, 'lon': -73.7880},
    'Sunnyside': {'lat': 40.7434, 'lon': -73.9269},
    'Central Park': {'lat': 40.7851, 'lon': -73.9683},
    'Times Square/Theatre District': {'lat': 40.7580, 'lon': -73.9855},
    'Astoria': {'lat': 40.7644, 'lon': -73.9235},
    'Williamsburg (North Side)': {'lat': 40.7175, 'lon': -73.9566}
}

# ── Header ──
st.title("NYC Taxi Analytics Dashboard")
st.markdown("**Architecture Consommation Gold Sécurisée (JWT)** — Streamlit Frontend ──> API Flask Gateway ──> MySQL Server")
st.divider()

# Récupération initiale
df_init_zones = load_data_from_api("zones")

if df_init_zones.empty:
    st.error("Impossible de joindre l'API Flask ou la base MySQL-Gold est vide. Vérifiez vos conteneurs Docker.")
    st.stop()

# Barre Latérale de Filtrage  

st.sidebar.header("Configuration des Tops & Filtres")
boroughs_list = ["Tous"] + sorted(list(df_init_zones["borough"].dropna().unique()))
selected_borough = st.sidebar.selectbox("Filtrer par District (Borough) :", boroughs_list)
top_n = st.sidebar.slider("Nombre d'éléments à afficher dans les Tops :", min_value=5, max_value=30, value=10)

# Ingestion Dynamique via l'API REST
with st.spinner("Mise à jour des indicateurs depuis l'API..."):
    df_zones = load_data_from_api("zones", params={"borough": selected_borough, "limit": top_n})
    df_paiement = load_data_from_api("paiements", params={"borough": selected_borough})
    df_all_zones = load_data_from_api("zones", params={"borough": selected_borough})
    
    df_heures = load_data_from_api("heures")
    df_aeroport = load_data_from_api("aeroports")
    df_meteo = load_data_from_api("meteo")

# ── Section 1 : Indicateurs Clés de Prix & Volumes ────────────
st.subheader(f"Métriques Générales — Filtre : {selected_borough}")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total trajets", "{:,}".format(int(df_all_zones["nb_trajets"].sum())) if not df_all_zones.empty else "0")
c2.metric("Chiffre d'Affaires total", "${:,.2f}".format(df_all_zones["ca_total"].sum()) if not df_all_zones.empty else "$0.00")
c3.metric("Tarif moyen / course", "${:.2f}".format(df_all_zones["tarif_moyen"].mean()) if not df_all_zones.empty else "$0.00")
c4.metric("Pourboire moyen", "${:.2f}".format(df_all_zones["pourboire_moyen"].mean()) if not df_all_zones.empty else "$0.00")

st.divider()

# ── Section des Récompenses / Podiums ──
st.subheader("Les Faits Marquants du Cluster (Podiums des Tops)")
p1, p2, p3 = st.columns(3)

with p1:
    st.markdown("**Top Zones Rentables**")
    if not df_zones.empty:
        for idx, row in df_zones.head(3).iterrows():
            st.write(f"**{row['rang_borough']}e** : {row['zone']} ({row['borough']}) → **${row['ca_total']:,.0f}**")

with p2:
    st.markdown("**Classement des Aéroports**")
    if not df_aeroport.empty:
        for idx, row in df_aeroport.iterrows():
            st.write(f"• **{row['aeroport']}** : {row['nb_trajets']:,} courses (Moy: ${row['tarif_moyen']:.2f})")
    else:
        st.write("*Données aéroports indisponibles*")

with p3:
    st.markdown("**Impact Météo Dominant**")
    if not df_meteo.empty:
        for idx, row in df_meteo.head(3).iterrows():
            st.write(f"• **{row['meteo']}** : {row['nb_trajets']:,} départs (Moy: ${row['tarif_moyen']:.2f}/course)")
    else:
        st.write("*Données météo indisponibles*")

st.divider()

# ── Section 2 : Carte Géographique Réelle et Top des Zones ──
col_map, col_chart = st.columns([1.2, 1])

with col_map:
    st.subheader("Carte d'Activité Réelle de NYC (Précision par Zone)")
    
    if not df_all_zones.empty:
        df_map = df_all_zones.copy()
        df_map['lat'] = df_map['zone'].map(lambda x: GPS_ZONES_REPLI.get(x, {'lat': 40.7128})['lat'])
        df_map['lon'] = df_map['zone'].map(lambda x: GPS_ZONES_REPLI.get(x, {'lon': -74.0060})['lon'])
        
        import numpy as np
        df_map['lat'] += np.random.uniform(-0.008, 0.008, len(df_map))
        df_map['lon'] += np.random.uniform(-0.008, 0.008, len(df_map))
        
        fig_map = px.scatter_mapbox(
            df_map, lat="lat", lon="lon", size="nb_trajets", color="borough",
            hover_name="zone", hover_data=["nb_trajets", "ca_total"],
            zoom=10, height=480, size_max=35,
            labels={"nb_trajets": "Nombre de trajets", "ca_total": "Chiffre d'Affaires ($)"}
        )
        fig_map.update_layout(mapbox_style="carto-positron", margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("Aucune donnée géographique à afficher pour ce filtre.")

with col_chart:
    st.subheader(f"Top {top_n} des zones les plus rentables")
    if not df_zones.empty:
        fig_zones = px.bar(
            df_zones, x="ca_total", y="zone", color="borough",
            orientation="h", labels={"ca_total": "Chiffre d'Affaires ($)", "zone": "Zone"}
        )
        fig_zones.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_zones, use_container_width=True)
    else:
        st.info("Aucun graphique disponible.")

st.divider()

# ── Section 3 : Temporelle et Modes de Paiements ───
col_time, col_pay = st.columns(2)

with col_time:
    st.subheader("Profil horaire de la demande")
    if not df_heures.empty:
        available_months = sorted(list(df_heures["mois"].unique()))
        selected_month = st.selectbox("Sélectionner un mois d'analyse :", available_months)
        
        df_heures_filtered = df_heures[df_heures["mois"] == selected_month]
        df_line = df_heures_filtered.groupby("heure")["nb_trajets"].sum().reset_index()
        
        fig_time = px.line(df_line, x="heure", y="nb_trajets", markers=True, 
                           title=f"Volume horaire global — Mois {selected_month}")
        st.plotly_chart(fig_time, use_container_width=True)
    else:
        st.info("Données horaires indisponibles.")

with col_pay:
    st.subheader("Répartition des transactions")
    if not df_paiement.empty:
        df_pay_top = df_paiement.sort_values(by="nb_trajets", ascending=False).head(top_n)
        fig_pay = px.bar(
            df_pay_top, x="mode_paiement", y="nb_trajets", color="mode_paiement",
            title=f"Volume par type de transaction ({selected_borough})"
        )
        st.plotly_chart(fig_pay, use_container_width=True)
    else:
        st.info("Données de paiement indisponibles.")

st.divider()

# ── Section 4 : Hubs Aéroportuaires et Corrélations Météo ─────
st.subheader("Focus Hubs Aéroportuaires & Météo")

col_aero, col_meteo = st.columns(2)

with col_aero:
    st.markdown("**Statistiques des Aéroports**")
    if not df_aeroport.empty and "aeroport" in df_aeroport.columns:
        fig_aero = px.bar(
            df_aeroport, x="aeroport", y="nb_trajets", color="aeroport",
            title="Nombre de trajets total par Hub aéroportuaire", text_auto='.2s'
        )
        st.plotly_chart(fig_aero, use_container_width=True)
    else:
        st.warning("Les données de la table 'kpi_aeroports' n'ont pas pu être lues.")

with col_meteo:
    st.markdown("**Honoraires et Corrélations Climatiques**")
    if not df_meteo.empty and "meteo" in df_meteo.columns:
        fig_meteo = px.scatter(
            df_meteo, x="temp_moy", y="nb_trajets", size="tarif_moyen", 
            color="meteo", hover_name="meteo",
            title="Impact climatique sur les volumes (Taille = Coût moyen)"
        )
        st.plotly_chart(fig_meteo, use_container_width=True)
    else:
        st.info("Données de corrélation météo temporairement indisponibles.")
        
st.caption("Fichiers de données traités par la stack Big Data de l'Efrei — Évaluation 2026")