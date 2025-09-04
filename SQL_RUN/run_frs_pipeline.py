import os
from sqlalchemy import text
from database.db_utils.connection import get_sqlalchemy_engine
from logger import logger

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "SQL")

def load_sql(filename: str) -> str:
    """
    Load SQL query from a file inside the /sql directory.
    """
    path = os.path.join(SQL_DIR, filename)
    with open(path, "r", encoding="utf-8") as file:
        return file.read()

def run_sql(conn, filename: str):
    """
    Load and execute a single SQL file using an open SQLAlchemy connection.
    """
    sql = load_sql(filename)
    conn.execute(text(sql))
    logger.info(f"Executed: {filename}")
    # print(f"Executed: {filename}")

def run_frs_SQL_pipeline():
    """
    Runs the full pipeline: 
    1. Create frs_master
    2. Init frs_master_data (only if not exists)
    3. Upsert frs_master_data from frs_master
    4. Enrich landfill info via haversine distance
    """
    engine = get_sqlalchemy_engine()
    with engine.begin() as conn:
        run_sql(conn, "create_frs_master.sql")
        run_sql(conn, "init_frs_master_data.sql")
        run_sql(conn, "upsert_frs_master_data.sql")
        run_sql(conn, "enrich_landfill.sql")

    logger.info("FRS master pipeline completed")
    print("FRS master pipeline completed.")

# For CLI execution
if __name__ == "__main__":
    run_frs_SQL_pipeline()
