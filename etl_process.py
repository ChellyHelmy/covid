import pandas as pd
import pygrametl
from pygrametl.datasources import CSVSource
from pygrametl.tables import Dimension, FactTable
import psycopg2
import glob

# --- 1. CLEANING DATA WITH PANDAS ---
print("Step 1: Cleaning data with Pandas...")

file_list = glob.glob("*.csv")
all_data = []

for file in file_list:
    # Skip the temporary final file if it exists to avoid infinite loops
    if "final_cleaned_covid.csv" in file: 
        continue
    
    try:
        df = pd.read_csv(file)
        # Standardize column names (fixes differences between files)
        df.columns = [c.replace('Country/Region', 'Country')
                       .replace('Date last updated', 'Last Update')
                       .replace('Death', 'Deaths') for c in df.columns]
        all_data.append(df)
        print(f"  - Loaded {file}")
    except Exception as e:
        print(f"  - Error loading {file}: {e}")

# Combine all CSVs into one big table
combined_df = pd.concat(all_data, ignore_index=True)

# Fill empty values (DWH requires no Nulls in measures)
combined_df['Province/State'] = combined_df['Province/State'].fillna('Unknown')
combined_df[['Confirmed', 'Deaths', 'Recovered', 'Suspected']] = combined_df[['Confirmed', 'Deaths', 'Recovered', 'Suspected']].fillna(0)

# FIX: Handle multiple date formats safely (mixed format)
combined_df['Last Update'] = pd.to_datetime(combined_df['Last Update'], format='mixed', dayfirst=False).dt.date

# Save a clean master file for the next step
combined_df.to_csv("final_cleaned_covid.csv", index=False)
print("Step 1 Complete: Created final_cleaned_covid.csv")

# --- 2. ETL WITH PYGRAMETL ---
print("Step 2: Loading data into PostgreSQL using pygramETL...")

# database connection (Replace 'helmy' with your pgAdmin password if different)
conn = psycopg2.connect(dbname="my_covid_db", user="postgres", password="helmy", host="localhost")
connection = pygrametl.ConnectionWrapper(conn)

# Define our Star Schema dimensions and facts
location_dim = Dimension(name='dim_location', key='location_id', 
                         attributes=['province_state', 'country_region'],
                         lookupatts=['province_state', 'country_region'])

date_dim = Dimension(name='dim_date', key='date_id', 
                     attributes=['full_date', 'day', 'month', 'year', 'quarter'],
                     lookupatts=['full_date'])

covid_fact = FactTable(name='fact_covid', 
                       keyrefs=['location_id', 'date_id'], 
                       measures=['confirmed', 'deaths', 'recovered', 'suspected'])

# Open the cleaned data source
data_source = CSVSource(open('final_cleaned_covid.csv', 'r'), delimiter=',')

for row in data_source:
    # A. HANDLE LOCATION (Manual Lookup then Insert)
    loc_id = location_dim.lookup({'province_state': row['Province/State'], 'country_region': row['Country']})
    if loc_id is None:
        loc_id = location_dim.insert({
            'province_state': row['Province/State'], 
            'country_region': row['Country']
        })
    
    # B. HANDLE DATE (Manual Lookup then Insert)
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
    
    # C. INSERT INTO FACT TABLE
    covid_fact.insert({
        'location_id': loc_id,
        'date_id': dt_id,
        'confirmed': row['Confirmed'],
        'deaths': row['Deaths'],
        'recovered': row['Recovered'],
        'suspected': row['Suspected']
    })

# Commit and close connection
connection.commit()
connection.close()

print("\nSuccess! Your Data Warehouse is now full.")
