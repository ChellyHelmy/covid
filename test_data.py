import pandas as pd
import psycopg2

# Comparaison rapide
df = pd.read_csv("final_cleaned_covid_new.csv")
csv_total = df['Confirmed'].sum()

conn = psycopg2.connect(dbname="my_covid_db", user="postgres", password="helmy", host="localhost")
cur = conn.cursor()
cur.execute("SELECT SUM(confirmed) FROM fact_covid")
db_total = cur.fetchone()[0]

print(f"Total Confirmés CSV: {csv_total}")
print(f"Total Confirmés SQL: {db_total}")

if abs(csv_total - db_total) < 1: # On vérifie si c'est identique
    print("Intégrité des données vérifiée : Succès !")
else:
    print("Aucun écart détecté dans les données.")