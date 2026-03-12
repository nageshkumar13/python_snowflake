import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """Create and return a Snowflake connection."""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def fetch_query(query: str):
    """Execute a query and return results as a list of rows."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(query)
        results = cur.fetchall()
        return results
    finally:
        cur.close()
        conn.close()
