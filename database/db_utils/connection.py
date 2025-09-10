from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging
from sqlalchemy.engine import URL
import psycopg2


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


load_dotenv()

def get_connection():
    """psycopg2 connection, works for Unix socket"""
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASS")
    CLOUD_SQL_CONNECTION = os.getenv("cloud_sql_connection")
    try:
        logger.info("Attempting to connect to DB via Unix socket...")

        socket_path = f"/cloudsql/{CLOUD_SQL_CONNECTION}"

        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=socket_path,  # This is the socket path
            port="5432"
        )

        logger.info("Database connection via socket PSYCOPG2 established successfully.")
        return conn

    except Exception as e:
        logger.error("Failed to establish socket connection to DB PSYCOPG2", exc_info=True)
        raise RuntimeError("Failed to connect with Database")


def get_engine():
    try:
        logger.info("Creating database engine...")

        url = URL.create(
            drivername="postgresql+pg8000",
            username=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASS'),
            database=os.environ.get('DB_NAME'),
            query={
                "unix_sock": f"/cloudsql/{os.environ.get('cloud_sql_connection')}/.s.PGSQL.5432"
            }
        )

        engine = create_engine(url)
        logger.info("Database engine created successfully.")
        return engine

    except Exception as e:
        logger.error("Failed to create database engine.", exc_info=True)
        raise

# -----------------------------------------------------------------------------
# Simple self-test (you can call this from your job's main or an ad-hoc route)
# -----------------------------------------------------------------------------
def test_connection():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()[0]
    print("Connected to PostgreSQL:", version)
    cur.close()
    conn.close()


if __name__ == "__main__":
    test_connection()
