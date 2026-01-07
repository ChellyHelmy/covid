import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(page_title="COVID-19 BI Dashboard", layout="wide")

st.title("📊 Dashboard Décisionnel COVID-19 (Juin 2020)")
st.markdown("Ce dashboard est connecté en temps réel à l'entrepôt de données PostgreSQL.")

# --- CONNEXION À LA BASE DE DONNÉES ---
@st.cache_resource
def init_connection():
    return psycopg2.connect(
        dbname="my_covid_db", 
        user="postgres", 
        password="helmy", 
        host="localhost"
    )

conn = init_connection()

# --- BARRE LATÉRALE (FILTRES) ---
st.sidebar.header("Filtres d'analyse")
query_pays = "SELECT DISTINCT country_region FROM dim_location ORDER BY country_region"
liste_pays = pd.read_sql(query_pays, conn)['country_region'].tolist()
pays_selectionne = st.sidebar.selectbox("Choisir un pays pour le zoom :", ["Tous les pays"] + liste_pays)

# --- KPI GLOBAUX (Chiffres clés) ---
st.subheader("Indicateurs Clés de Performance (KPI)")
query_kpi = "SELECT SUM(confirmed), SUM(deaths), SUM(recovered) FROM fact_covid"
df_kpi = pd.read_sql(query_kpi, conn)

col1, col2, col3 = st.columns(3)
col1.metric("Total Confirmés", f"{int(df_kpi.iloc[0,0]):,}")
col2.metric("Total Décès", f"{int(df_kpi.iloc[0,1]):,}", delta_color="inverse")
col3.metric("Total Guérisons", f"{int(df_kpi.iloc[0,2]):,}")

# --- GRAPHIQUES ---
st.divider()

# Ligne 1 : Évolution et Top Pays
c1, c2 = st.columns(2)

with c1:
    st.subheader("📈 Évolution temporelle")
    q1 = "SELECT d.full_date, SUM(f.confirmed) as total FROM fact_covid f JOIN dim_date d ON f.date_id = d.date_id GROUP BY d.full_date ORDER BY d.full_date"
    df1 = pd.read_sql(q1, conn)
    fig1 = px.line(df1, x='full_date', y='total', title="Progression mondiale des cas")
    st.plotly_chart(fig1, use_container_width=True)

with c2:
    st.subheader("🏆 Top 10 des Pays (Cas)")
    q2 = "SELECT l.country_region, SUM(f.confirmed) as total FROM fact_covid f JOIN dim_location l ON f.location_id = l.location_id GROUP BY l.country_region ORDER BY total DESC LIMIT 10"
    df2 = pd.read_sql(q2, conn)
    fig2 = px.bar(df2, x='total', y='country_region', orientation='h', title="Pays les plus touchés", color='total')
    st.plotly_chart(fig2, use_container_width=True)

# Ligne 2 : Mortalité et Risque
c3, c4 = st.columns(2)

with c3:
    st.subheader("💀 Taux de Létalité (%)")
    q4 = """SELECT l.country_region, (SUM(f.deaths)::float / NULLIF(SUM(f.confirmed), 0)) * 100 as rate 
            FROM fact_covid f JOIN dim_location l ON f.location_id = l.location_id 
            GROUP BY l.country_region HAVING SUM(f.confirmed) > 10000 ORDER BY rate DESC LIMIT 10"""
    df4 = pd.read_sql(q4, conn)
    fig4 = px.bar(df4, x='country_region', y='rate', title="Top 10 - Taux de mortalité par pays", color='rate', color_continuous_scale='Reds')
    st.plotly_chart(fig4, use_container_width=True)

with c4:
    st.subheader("🎉 Top 10 des Guérisons")
    q5 = "SELECT l.country_region, SUM(f.recovered) as total FROM fact_covid f JOIN dim_location l ON f.location_id = l.location_id GROUP BY l.country_region ORDER BY total DESC LIMIT 10"
    df5 = pd.read_sql(q5, conn)
    fig5 = px.pie(df5, values='total', names='country_region', title="Répartition des guérisons par pays")
    st.plotly_chart(fig5, use_container_width=True)

# --- ZOOM PAR PAYS (Optionnel) ---
if pays_selectionne != "Tous les pays":
    st.divider()
    st.subheader(f"🔍 Zoom sur : {pays_selectionne}")
    q_zoom = f"SELECT SUM(f.confirmed), SUM(f.deaths), SUM(f.recovered) FROM fact_covid f JOIN dim_location l ON f.location_id = l.location_id WHERE l.country_region = '{pays_selectionne}'"
    res = pd.read_sql(q_zoom, conn)
    
    z1, z2, z3 = st.columns(3)
    z1.write(f"**Confirmés :** {int(res.iloc[0,0]):,}")
    z2.write(f"**Décès :** {int(res.iloc[0,1]):,}")
    z3.write(f"**Guérisons :** {int(res.iloc[0,2]):,}")