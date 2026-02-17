from snowflake_client import get_connection
from .sql_loader import load_sql
from .prompt_loader import load_prompt

PIPELINE_ORDER = [
    "00_setup_schemas.sql",
    "01_raw_create_table.sql",
    "02_raw_seed_sample.sql",
    "03_ai_create_table.sql",
]

AI_ENRICH_FILE = "04_ai_enrich.sql"
ANALYTICS_FILES = [
    "05_analytics_views.sql",
]


def run_simple_sql(filename):
    query = load_sql(filename)

    # Split statements by semicolon
    statements = [stmt.strip() for stmt in query.split(";") if stmt.strip()]

    conn = get_connection()
    cur = conn.cursor()

    try:
        for stmt in statements:
            cur.execute(stmt)
    finally:
        cur.close()
        conn.close()


def run_ai_enrichment():
    print("Running AI enrichment...")

    model = "snowflake-arctic"
    prompt_version = "v1"

    prompt_template = load_prompt("ticket_enrichment_prompt.txt")

    conn = get_connection()
    cur = conn.cursor()

    try:
        enrich_sql = load_sql(AI_ENRICH_FILE)

        cur.execute(
            enrich_sql,
            (
                model,
                prompt_template,
                model,
                prompt_template,
                model,
                prompt_version,
                model,
                prompt_version
            )
        )

    finally:
        cur.close()
        conn.close()


def run_pipeline():
    for file in PIPELINE_ORDER:
        print(f"Running {file}")
        run_simple_sql(file)

    run_ai_enrichment()

    for file in ANALYTICS_FILES:
        run_simple_sql(file)

    print("Pipeline complete.")
