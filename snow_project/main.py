from snowflake_client import fetch_dataframe

def main():
    query = "SELECT * FROM YOUR_TABLE LIMIT 10"
    df = fetch_dataframe(query)
    print(df)


if __name__ == "__main__":
    main()
