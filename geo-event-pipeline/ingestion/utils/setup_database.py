"""
Database setup utilities.
Creates RAW layer schemas and tables in DuckDB.
"""

import duckdb
from pathlib import Path


def get_project_root() -> Path:
    """Return the geo-event-pipeline root directory."""
    # ingestion/utils/setup_database.py -> go up 3 levels
    return Path(__file__).resolve().parent.parent.parent


def setup_raw_layer(db_path: str = None) -> None:
    """
    Execute the RAW layer DDL from sql/setup/01_create_raw_tables.sql.
    """
    root = get_project_root()

    if db_path is None:
        db_path = root / "data" / "warehouse.duckdb"

    sql_file = root / "sql" / "setup" / "01_create_raw_tables.sql"

    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")

    conn = duckdb.connect(str(db_path))
    conn.execute(sql_file.read_text())

    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'raw' "
        "ORDER BY table_name"
    ).fetchall()

    print(f"RAW tables in {db_path}:")
    for t in tables:
        print(f"  ✓ {t[0]}")

    conn.close()


if __name__ == "__main__":
    setup_raw_layer()