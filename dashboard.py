import pandas as pd
import matplotlib.pyplot as plt
import psycopg2

# 1. Connect to your Data Warehouse
conn = psycopg2.connect(
    dbname="my_covid_db", 
    user="postgres", 
    password="helmy", 
    host="localhost"
)

print("Connected to Warehouse. Generating charts...")

# --- CHART 1: Evolution of Confirmed Cases Over Time ---
query1 = """
    SELECT d.full_date, SUM(f.confirmed) as total_confirmed
    FROM fact_covid f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.full_date 
    ORDER BY d.full_date
"""
df_time = pd.read_sql(query1, conn)

plt.figure(figsize=(10, 6))
plt.plot(df_time['full_date'], df_time['total_confirmed'], marker='o', color='tab:red', linewidth=2)
plt.title('Global Evolution of Confirmed COVID-19 Cases', fontsize=14)
plt.xlabel('Date')
plt.ylabel('Number of Cases')
plt.grid(True, linestyle='--', alpha=0.7)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('evolution_cases.png')
print("- Saved: evolution_cases.png")

# --- CHART 2: Top 10 Countries by Confirmed Cases ---
query2 = """
    SELECT l.country_region, SUM(f.confirmed) as total_confirmed
    FROM fact_covid f
    JOIN dim_location l ON f.location_id = l.location_id
    GROUP BY l.country_region
    ORDER BY total_confirmed DESC
    LIMIT 10
"""
df_country = pd.read_sql(query2, conn)

plt.figure(figsize=(10, 6))
plt.bar(df_country['country_region'], df_country['total_confirmed'], color='tab:blue')
plt.title('Top 10 Countries by Total Confirmed Cases', fontsize=14)
plt.xlabel('Country')
plt.ylabel('Confirmed Cases')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('top_countries.png')
print("- Saved: top_countries.png")

# --- CHART 3: Mortality Rate Analysis (Deaths vs Confirmed) ---
query3 = """
    SELECT l.country_region, 
           SUM(f.deaths) as total_deaths, 
           SUM(f.confirmed) as total_confirmed
    FROM fact_covid f
    JOIN dim_location l ON f.location_id = l.location_id
    GROUP BY l.country_region
    HAVING SUM(f.confirmed) > 100
    ORDER BY (SUM(f.deaths)/SUM(f.confirmed)) DESC
    LIMIT 5
"""
df_mortality = pd.read_sql(query3, conn)

plt.figure(figsize=(8, 8))
plt.pie(df_mortality['total_deaths'], labels=df_mortality['country_region'], autopct='%1.1f%%', startangle=140)
plt.title('Distribution of Deaths in High-Impact Countries')
plt.tight_layout()
plt.savefig('death_distribution.png')
print("- Saved: death_distribution.png")

conn.close()
print("\nDashboard Complete! Check your folder for the PNG images.")