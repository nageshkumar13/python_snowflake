from pathlib import Path
from snowflake_client import fetch_query

def load_sql(file_path: str) -> str:
    """Load SQL query from a .sql file."""
    return Path(file_path).read_text()


def main():
    sql_path = Path(__file__).parent / "queries" / "users_sample.sql"
    query = load_sql(sql_path)

    results = fetch_query(query)

    for row in results:
        print(row)


if __name__ == "__main__":
    main()
