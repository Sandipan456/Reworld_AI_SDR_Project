import os
import pandas as pd
from sqlalchemy import text
from database.db_utils.connection import get_engine
from logger import logger

# Directory containing your SQL files
SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "SQL")

def load_sql(filename: str) -> str:
    """Load SQL query from file."""
    path = os.path.join(SQL_DIR, filename)
    with open(path, "r", encoding="utf-8") as file:
        return file.read()

def run_sql(conn, filename: str):
    """Execute all SQL statements in a file."""
    sql = load_sql(filename)
    for statement in sql.strip().split(';'):
        if statement.strip():
            conn.execute(text(statement))
    logger.info(f"Executed: {filename}")

def primary_key_exists(conn, table: str, constraint_name: str) -> bool:
    """Check if a primary key constraint exists."""
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass(:table)
              AND conname = :constraint
              AND contype = 'p'
        );
    """
    result = conn.execute(
        text(query),
        {"table": table, "constraint": constraint_name}
    )
    return result.scalar()

def add_primary_key_if_missing(conn):
    """Add the primary key only if it doesn't already exist."""
    table = "frs_master_data"
    constraint = "frs_master_data_pk"
    if primary_key_exists(conn, table, constraint):
        logger.info(f"Primary key already exists on {table}")
    else:
        logger.info(f"🛠 Adding primary key to {table}...")
        conn.execute(text(f"""
            ALTER TABLE {table}
            ADD CONSTRAINT {constraint} PRIMARY KEY (registry_id);
        """))
        logger.info("Primary key added.")

def run_frs_SQL_pipeline():
    """
    Runs the full pipeline with transaction handling and primary key check.
    """
    engine = get_engine()

    sql_files = [
        "drop_frs_master.sql",
        "create_frs_master.sql",
        "alter_frs_master.sql",
        "init_frs_master_data.sql",
        "add_primary_key_to_frs_master.sql",  # Handled differently
        "upsert_frs_master_data.sql",
        "enrich_landfill.sql"
    ]

    engine.dispose()

    for filename in sql_files:
        with engine.connect() as conn:
            if filename == "add_primary_key_to_frs_master.sql":
                # Separate logic, no try block
                with conn.begin():
                    add_primary_key_if_missing(conn)
                logger.info(f"Successfully executed: {filename}")
            else:
                try:
                    with conn.begin():
                        run_sql(conn, filename)
                    logger.info(f"Successfully executed: {filename}")
                except Exception as e:
                    logger.error(f"Failed to execute {filename}: {e}")
                    print(f"Failed to execute {filename}: {e}")
                    break

    logger.info("🏁 FRS master pipeline completed")
    print("🏁 FRS master pipeline completed")

# For CLI execution
if __name__ == "__main__":
    run_frs_SQL_pipeline()
