import pandas as pd
import numpy as np  # <--- Added numpy to handle Infinity
import os
import sqlalchemy
from sqlalchemy import create_engine

# --- CONFIGURATION ---
# UPDATE THIS with your exact Server Name
SERVER_NAME = 'OMEN\SQLEXPRESS'
DATABASE_NAME = 'VaccineDB'

# Define File Paths
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
CLEAN_DIR = os.path.join(PROJECT_ROOT, "data_cleaned")


def get_db_engine():
    conn_str = (
        f"mssql+pyodbc://@{SERVER_NAME}/{DATABASE_NAME}?"
        "driver=ODBC+Driver+17+for+SQL+Server&"
        "trusted_connection=yes"
    )
    return create_engine(conn_str)


def load_data():
    print("--- Starting Data Pipeline to SQL Server ---")

    # 1. Read CSVs
    try:
        print("Reading CSV files...")
        df_cov = pd.read_csv(os.path.join(CLEAN_DIR, "coverage_clean.csv"))
        df_inc = pd.read_csv(os.path.join(CLEAN_DIR, "incidence_clean.csv"))
        df_cases = pd.read_csv(os.path.join(CLEAN_DIR, "cases_clean.csv"))
        df_intro = pd.read_csv(os.path.join(CLEAN_DIR, "vaccine_intro_clean.csv"))
        df_sched = pd.read_csv(os.path.join(CLEAN_DIR, "vaccine_schedule_clean.csv"))
    except FileNotFoundError:
        print("ERROR: CSV files not found. Did you run clean_and_prepare.py?")
        return

    # --- SAFETY FIX 1: Smart Rename ---
    dfs = [df_cov, df_inc, df_cases, df_intro, df_sched]
    for df in dfs:
        rename_map = {
            'Country Name': 'Name', 'CountryName': 'Name', 'Country': 'Name',
            'ISO_3_Code': 'Code', 'ISO 3 Code': 'Code', 'code': 'Code'
        }
        df.rename(columns=rename_map, inplace=True)

    # --- SAFETY FIX 2: Kill Infinity (The cause of your error) ---
    print("Sanitizing data (Removing Infinity/NaN)...")
    for df in dfs:
        # Replace Python 'inf' with NaN, then NaN with None (which becomes SQL NULL)
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        # Fill NaN with None is not strictly needed for to_sql but safer for some drivers
        df.fillna(np.nan, inplace=True)

        # 2. Connect
    try:
        engine = get_db_engine()
        connection = engine.connect()
        print("Successfully connected to SQL Server.")
    except Exception as e:
        print(f"ERROR: Could not connect to SQL Server.\nDetails: {e}")
        return

    # 3. Load Country Dimension
    print("Building Country Dimension...")
    valid_dfs = [df for df in dfs if 'Code' in df.columns and 'Name' in df.columns]

    if valid_dfs:
        all_countries = pd.concat([d[['Code', 'Name']] for d in valid_dfs])
        all_countries = all_countries.drop_duplicates(subset=['Code']).rename(
            columns={'Code': 'CountryCode', 'Name': 'CountryName'})
        all_countries.to_sql('dim_country', engine, if_exists='replace', index=False)
        print(f" -> Loaded {len(all_countries)} rows into 'dim_country'")

    # 4. Load Fact Tables

    # Coverage
    print("Loading Fact_Coverage...")
    df_cov.rename(columns={'Code': 'CountryCode', 'Target number': 'TargetNumber', 'Coverage': 'CoverageValue'},
                  inplace=True)
    cols = ['CountryCode', 'Year', 'Antigen', 'Doses', 'TargetNumber', 'CoverageValue']
    df_cov = df_cov[[c for c in cols if c in df_cov.columns]]  # Safety filter
    df_cov.to_sql('fact_coverage', engine, if_exists='replace', index=False)
    print(f" -> Loaded {len(df_cov)} rows into 'fact_coverage'")

    # Incidence
    print("Loading Fact_Incidence...")
    df_inc.rename(columns={'Code': 'CountryCode', 'Incidence rate': 'IncidenceRate'}, inplace=True)
    cols = ['CountryCode', 'Year', 'Disease', 'IncidenceRate', 'Denominator']
    df_inc = df_inc[[c for c in cols if c in df_inc.columns]]
    df_inc.to_sql('fact_incidence', engine, if_exists='replace', index=False)
    print(f" -> Loaded {len(df_inc)} rows into 'fact_incidence'")

    # Cases
    print("Loading Fact_Cases...")
    df_cases.rename(columns={'Code': 'CountryCode'}, inplace=True)
    cols = ['CountryCode', 'Year', 'Disease', 'Cases']
    df_cases = df_cases[[c for c in cols if c in df_cases.columns]]
    df_cases.to_sql('fact_cases', engine, if_exists='replace', index=False)
    print(f" -> Loaded {len(df_cases)} rows into 'fact_cases'")

    connection.close()
    print("\nSUCCESS! All data has been moved to SQL Server.")


load_data()
if __name__ == "__main__":
    pass
