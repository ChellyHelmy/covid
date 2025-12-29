import pandas as pd
import pygrametl
from pygrametl.datasources import CSVSource
from pygrametl.tables import Dimension, FactTable
import psycopg2

# --- 1. CLEANING DATA WITH PANDAS ---
print("Step 1: Cleaning the new dataset...")

file_name = "forecasts_custom_prophet.csv"
df = pd.read_csv(file_name)

# FIX: Supprimer les lignes où ObservationDate est "0" ou contient seulement des 0
# Ces lignes empêchent la conversion des dates
df = df[df['ObservationDate'].astype(str) != '0']

# Renommer les colonnes pour la cohérence
df = df.rename(columns={
    'ObservationDate': 'Last Update',
    'Country/Region': 'Country'
})

# Remplir les valeurs manquantes
df['Province/State'] = df['Province/State'].fillna('Unknown')
df['Suspected'] = 0  # Cette colonne n'existe pas dans ce fichier
df[['Confirmed', 'Deaths', 'Recovered']] = df[['Confirmed', 'Deaths', 'Recovered']].fillna(0)

# FIX: Utiliser errors='coerce' pour transformer les dates invalides restantes en NaT (Not a Time)
# Puis supprimer ces NaT
df['Last Update'] = pd.to_datetime(df['Last Update'], errors='coerce')
df = df.dropna(subset=['Last Update'])
df['Last Update'] = df['Last Update'].dt.date

# Sauvegarder la version propre
df.to_csv("final_cleaned_covid_new.csv", index=False)
print(f"Step 1 Complete: Processed {len(df)} valid rows.")

# --- 2. ETL WITH PYGRAMETL ---
print("Step 2: Loading data into PostgreSQL...")

conn = psycopg2.connect(dbname="my_covid_db", user="postgres", password="helmy", host="localhost")
connection = pygrametl.ConnectionWrapper(conn)

location_dim = Dimension(name='dim_location', key='location_id', 
                         attributes=['province_state', 'country_region'],
                         lookupatts=['province_state', 'country_region'])

date_dim = Dimension(name='dim_date', key='date_id', 
                     attributes=['full_date', 'day', 'month', 'year', 'quarter'],
                     lookupatts=['full_date'])

covid_fact = FactTable(name='fact_covid', 
                       keyrefs=['location_id', 'date_id'], 
                       measures=['confirmed', 'deaths', 'recovered', 'suspected'])

data_source = CSVSource(open('final_cleaned_covid_new.csv', 'r'), delimiter=',')

for row in data_source:
    # Location Lookup/Insert
    loc_id = location_dim.lookup({'province_state': row['Province/State'], 'country_region': row['Country']})
    if loc_id is None:
        loc_id = location_dim.insert({'province_state': row['Province/State'], 'country_region': row['Country']})
    
    # Date Lookup/Insert
    dt_val = pd.to_datetime(row['Last Update'])
    dt_id = date_dim.lookup({'full_date': dt_val.date()})
    if dt_id is None:
        dt_id = date_dim.insert({
            'full_date': dt_val.date(),
            'day': dt_val.day,
            'month': dt_val.month,
            'year': dt_val.year,
            'quarter': (dt_val.month - 1) // 3 + 1
        })
    
    # Fact Insert
    covid_fact.insert({
        'location_id': loc_id,
        'date_id': dt_id,
        'confirmed': row['Confirmed'],
        'deaths': row['Deaths'],
        'recovered': row['Recovered'],
        'suspected': row['Suspected']
    })

connection.commit()
connection.close()
print("Success! Data loaded without the '0' values.")