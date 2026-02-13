from snowflake_client import fetch_query
from .sql_loader import load_sql


PIPELINE_ORDER = [
    "00_setup_schemas.sql",
    "01_raw_create_table.sql",
    "02_raw_seed_sample.sql",
    "03_ai_create_table.sql",
    "04_ai_enrich.sql",
    "05_analytics_views.sql",
]


ANALYTICS_QUERY_FILE = "06_analytics_queries.sql"


def run_pipeline():
    for file in PIPELINE_ORDER:
        print(f"Running {file}...")
        query = load_sql(file)
        fetch_query(query)

    print("Pipeline setup complete.")


def run_analytics():
    print("Running analytics queries...")
    query = load_sql(ANALYTICS_QUERY_FILE)
    results = fetch_query(query)

    for row in results:
        print(row)
