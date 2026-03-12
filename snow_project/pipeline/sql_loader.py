from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
QUERIES_DIR = BASE_DIR / "queries"


def load_sql(filename: str) -> str:
    file_path = QUERIES_DIR / filename
    return file_path.read_text()
