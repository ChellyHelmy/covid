import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
import sys

# 1. Connexion à la base de données PostgreSQL
try:
    conn = psycopg2.connect(
        dbname="my_covid_db", 
        user="postgres", 
        password="helmy", 
        host="localhost"
    )
    print("Connexion réussie au Warehouse. Génération des graphiques...")
except Exception as e:
    print(f"Erreur de connexion : {e}")
    sys.exit()

# --- GRAPHIQUE 1 : Évolution temporelle (Ligne) ---
# Ce graphique montre la progression globale des cas confirmés
query1 = """
    SELECT d.full_date, SUM(f.confirmed) as total_confirmed
    FROM fact_covid f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.full_date 
    ORDER BY d.full_date
"""
df_time = pd.read_sql(query1, conn)

plt.figure(figsize=(12, 6))
plt.plot(df_time['full_date'], df_time['total_confirmed'], marker='o', color='firebrick', linewidth=2)
plt.title('Évolution Mondiale des Cas Confirmés (Juin 2020)', fontsize=14)
plt.xlabel('Date')
plt.ylabel('Nombre total de cas')
plt.grid(True, linestyle='--', alpha=0.5)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('evolution_juin.png')
print("- Graphique 1 sauvegardé : evolution_juin.png")

# --- GRAPHIQUE 2 : Top 10 des Pays les plus touchés (Barres) ---
# Analyse comparative des pays ayant le plus grand nombre de cas
query2 = """
    SELECT l.country_region, SUM(f.confirmed) as total_confirmed
    FROM fact_covid f
    JOIN dim_location l ON f.location_id = l.location_id
    GROUP BY l.country_region
    ORDER BY total_confirmed DESC
    LIMIT 10
"""
df_country = pd.read_sql(query2, conn)

plt.figure(figsize=(12, 6))
plt.bar(df_country['country_region'], df_country['total_confirmed'], color='teal')
plt.title('Top 10 des Pays par Nombre de Cas Confirmés', fontsize=14)
plt.ylabel('Cas Confirmés')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('top_pays_juin.png')
print("- Graphique 2 sauvegardé : top_pays_juin.png")

# --- GRAPHIQUE 3 : Répartition de la Mortalité (Camembert) ---
# Regroupement intelligent : Top 5 pays et les autres en catégorie "Autres"
query3 = """
    SELECT l.country_region, SUM(f.deaths) as total_deaths
    FROM fact_covid f
    JOIN dim_location l ON f.location_id = l.location_id
    GROUP BY l.country_region
    ORDER BY total_deaths DESC
"""
df_deaths = pd.read_sql(query3, conn)

if not df_deaths.empty:
    # On garde les 5 premiers et on somme le reste
    top_5 = df_deaths.head(5)
    others_val = df_deaths.iloc[5:]['total_deaths'].sum()
    others_row = pd.DataFrame({'country_region': ['Autres'], 'total_deaths': [others_val]})
    df_plot = pd.concat([top_5, others_row])

    plt.figure(figsize=(10, 8))
    # 'explode' pour détacher légèrement la part la plus importante
    explode = [0.1 if i == 0 else 0 for i in range(len(df_plot))]
    
    plt.pie(df_plot['total_deaths'], 
            labels=df_plot['country_region'], 
            autopct='%1.1f%%', 
            startangle=140, 
            colors=plt.cm.Paired.colors,
            explode=explode,
            shadow=True)
    
    plt.title('Répartition Mondiale des Décès (Juin 2020)', fontsize=14)
    plt.axis('equal') 
    plt.tight_layout()
    plt.savefig('repartition_deces_juin.png')
    print("- Graphique 3 sauvegardé : repartition_deces_juin.png")
# --- GRAPHIQUE 4 : Taux de Létalité par Pays (Analyse de Risque) ---
# Le taux de létalité montre le pourcentage de décès parmi les cas confirmés
query4 = """
    SELECT l.country_region, 
           (SUM(f.deaths)::float / NULLIF(SUM(f.confirmed), 0)) * 100 as taux_mortalite
    FROM fact_covid f
    JOIN dim_location l ON f.location_id = l.location_id
    GROUP BY l.country_region
    HAVING SUM(f.confirmed) > 50000 -- On filtre les pays avec assez de données
    ORDER BY taux_mortalite DESC
    LIMIT 10
"""
df_risk = pd.read_sql(query4, conn)

plt.figure(figsize=(12, 6))
plt.barh(df_risk['country_region'], df_risk['taux_mortalite'], color='orange')
plt.title('Top 10 des Pays par Taux de Létalité (%)', fontsize=14)
plt.xlabel('Taux de Mortalité (%)')
plt.gca().invert_yaxis() # Pour avoir le plus haut en haut
plt.tight_layout()
plt.savefig('taux_letalite.png')
print("- Graphique 4 sauvegardé : taux_letalite.png")

# --- GRAPHIQUE 5 : Top 10 des Pays par Guérisons (Analyse de Rétablissement) ---
query5 = """
    SELECT l.country_region, SUM(f.recovered) as total_guéris
    FROM fact_covid f
    JOIN dim_location l ON f.location_id = l.location_id
    GROUP BY l.country_region
    ORDER BY total_guéris DESC
    LIMIT 10
"""
df_recov = pd.read_sql(query5, conn)

plt.figure(figsize=(12, 6))
plt.bar(df_recov['country_region'], df_recov['total_guéris'], color='limegreen')
plt.title('Top 10 des Pays par Nombre de Guérisons', fontsize=14)
plt.ylabel('Nombre de Personnes Guéries')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('top_guerisons.png')
print("- Graphique 5 sauvegardé : top_guerisons.png")

conn.close()
print("\nTableau de bord terminé ! Vérifiez vos fichiers images.")