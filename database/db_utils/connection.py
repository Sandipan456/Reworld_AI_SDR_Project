import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.environ.get("DB_USER")
DB_NAME = os.environ.get("DB_NAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

def get_connection():
    try:
        conn = psycopg2.connect(
            dbname = DB_NAME,
            user = DB_USER,
            password = DB_PASSWORD,
            host = "localhost",
            port = "5432"
        )
        return conn
    except Exception as e:
        print("Failed to establish connection with DB:", e)
        raise "Failed to connect with Database"
    
def test_connection():
    conn = get_connection()
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print("Connected to PostgreSQL:", version[0])
        cur.close()
        conn.close()
    else:
        print("Could not connect.")



if __name__ == "__main__":
    test_connection()