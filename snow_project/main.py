from snowflake_client import fetch_query

def main():
    query = "SELECT * FROM users LIMIT 10"
    results = fetch_query(query)

    for row in results:
        print(row)


if __name__ == "__main__":
    main()
