import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

# --- 1. CONFIGURATION DE LA PAGE ---
# Définit le titre de l'onglet et la mise en page large (wide)
st.set_page_config(
    page_title="COVID-19 BI Dashboard",
    page_icon="📊",
    layout="wide"
)

# --- 2. STYLE PERSONNALISÉ (CSS) ---
# Améliore l'apparence visuelle (couleurs de fond, ombres des cartes)
st.markdown("""
    <style>
    .main { background-color: #f5f7f9; }
    .stMetric {
        background-color: #000000;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    </style>
    """, unsafe_allow_html=True)

st.title("📊 Dashboard Décisionnel COVID-19 (Juin 2020)")
st.markdown("---")

# --- 3. CONNEXION À LA BASE DE DONNÉES ---
# @st.cache_resource permet de garder la connexion ouverte sans la recharger à chaque clic
@st.cache_resource
def init_connection():
    try:
        return psycopg2.connect(
            dbname="my_covid_db", 
            user="postgres", 
            password="helmy", 
            host="localhost"
        )
    except Exception as e:
        st.error(f"Erreur de connexion : {e}")
        return None

conn = init_connection()

if conn:
    # --- 4. BARRE LATÉRALE (SIDEBAR) - SYSTÈME DE RECHERCHE ---
    st.sidebar.header("🔍 Recherche par Pays")
    
    # Récupération dynamique de la liste des pays depuis dim_location
    query_liste = "SELECT DISTINCT country_region FROM dim_location ORDER BY country_region"
    liste_pays = pd.read_sql(query_liste, conn)['country_region'].tolist()
    
    pays_choisi = st.sidebar.selectbox("Sélectionnez un pays :", liste_pays)

    # Affichage des statistiques spécifiques au pays sélectionné
    if pays_choisi:
        query_stats = """
            SELECT SUM(f.confirmed), SUM(f.deaths), SUM(f.recovered) 
            FROM fact_covid f 
            JOIN dim_location l ON f.location_id = l.location_id 
            WHERE l.country_region = %s
        """
        cur = conn.cursor()
        cur.execute(query_stats, (pays_choisi,))
        res = cur.fetchone()
        
        if res and res[0] is not None:
            st.sidebar.markdown(f"### 📍 {pays_choisi}")
            st.sidebar.metric("Confirmés", f"{int(res[0]):,}")
            st.sidebar.metric("Décès", f"{int(res[1]):,}")
            st.sidebar.metric("Guérisons", f"{int(res[2]):,}")
            # Calcul du taux de guérison
            taux = (res[2] / res[0] * 100) if res[0] > 0 else 0
            st.sidebar.write(f"📈 **Taux de guérison :** {taux:.2f}%")

    # --- 5. INDICATEURS GLOBAUX (KPI) ---
    st.subheader("🌐 Vue d'ensemble mondiale")
    q_kpi = "SELECT SUM(confirmed), SUM(deaths), SUM(recovered) FROM fact_covid"
    df_kpi = pd.read_sql(q_kpi, conn)
    
    # Affichage sur 3 colonnes pour un look "Dashboard"
    k1, k2, k3 = st.columns(3)
    k1.metric("Total Confirmés", f"{int(df_kpi.iloc[0,0]):,}")
    k2.metric("Total Décès", f"{int(df_kpi.iloc[0,1]):,}")
    k3.metric("Total Guérisons", f"{int(df_kpi.iloc[0,2]):,}")
    
    st.markdown("---")

    # --- 6. GRAPHIQUES (RESTITUTION VISUELLE) ---
    
    # LIGNE 1 : Évolution temporelle et Top Pays
    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        st.subheader("📈 Évolution temporelle")
        q1 = """
            SELECT d.full_date, SUM(f.confirmed) as total 
            FROM fact_covid f JOIN dim_date d ON f.date_id = d.date_id 
            GROUP BY d.full_date ORDER BY d.full_date
        """
        df_time = pd.read_sql(q1, conn)
        # Graphique linéaire Plotly
        fig1 = px.line(df_time, x='full_date', y='total', title="Progression des cas confirmés")
        fig1.update_traces(line_color='firebrick')
        st.plotly_chart(fig1, use_container_width=True)

    with row1_col2:
        st.subheader("🏆 Top 10 des Pays")
        q2 = """
            SELECT l.country_region, SUM(f.confirmed) as total 
            FROM fact_covid f JOIN dim_location l ON f.location_id = l.location_id 
            GROUP BY l.country_region ORDER BY total DESC LIMIT 10
        """
        df_country = pd.read_sql(q2, conn)
        # Graphique à barres Plotly
        fig2 = px.bar(df_country, x='country_region', y='total', color='total', 
                     color_continuous_scale='Teal')
        st.plotly_chart(fig2, use_container_width=True)

    # LIGNE 2 : Répartition Décès et Taux Létalité
    row2_col1, row2_col2 = st.columns(2)

    with row2_col1:
        st.subheader("🍕 Répartition des Décès")
        q3 = """
            SELECT l.country_region, SUM(f.deaths) as total_deaths
            FROM fact_covid f JOIN dim_location l ON f.location_id = l.location_id
            GROUP BY l.country_region ORDER BY total_deaths DESC
        """
        df_deaths = pd.read_sql(q3, conn)
        # Création de la catégorie "Autres" pour le Pie Chart
        top_5 = df_deaths.head(5)
        others_val = df_deaths.iloc[5:]['total_deaths'].sum()
        others_row = pd.DataFrame({'country_region': ['Autres'], 'total_deaths': [others_val]})
        df_pie = pd.concat([top_5, others_row])
        
        # Graphique en camembert (Donut chart)
        fig3 = px.pie(df_pie, values='total_deaths', names='country_region', 
                     hole=0.4, color_discrete_sequence=px.colors.qualitative.Plotly)
        st.plotly_chart(fig3, use_container_width=True)

    with row2_col2:
        st.subheader("⚠️ Taux de Létalité (%)")
        q4 = """
            SELECT l.country_region, (SUM(f.deaths)::float / NULLIF(SUM(f.confirmed), 0)) * 100 as rate 
            FROM fact_covid f JOIN dim_location l ON f.location_id = l.location_id 
            GROUP BY l.country_region HAVING SUM(f.confirmed) > 50000 ORDER BY rate DESC LIMIT 10
        """
        df_risk = pd.read_sql(q4, conn)
        # Graphique à barres horizontales
        fig4 = px.bar(df_risk, x='rate', y='country_region', orientation='h',
                     color='rate', color_continuous_scale='Reds')
        fig4.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig4, use_container_width=True)

    st.success("✅ Données extraites avec succès du Data Warehouse PostgreSQL.")

else:
    st.error("Impossible de se connecter à la base de données. Vérifiez PostgreSQL.")