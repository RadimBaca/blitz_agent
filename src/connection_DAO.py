import os
import sqlite3

DB_DIR = "db"
DB_PATH = os.path.join(DB_DIR, "results.db")
INIT_SQL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "init_results_db.sql")


def _ensure_db():
    """Ensure the database exists and is initialized"""
    if not os.path.exists(DB_DIR):
        os.makedirs(DB_DIR)
    if not os.path.exists(DB_PATH):
        with open(INIT_SQL_PATH, "r", encoding="utf-8") as f:
            sql = f.read()
        conn = sqlite3.connect(DB_PATH)
        try:
            for stmt in sql.split(";"):
                if stmt.strip():
                    conn.execute(stmt)
            conn.commit()
        finally:
            conn.close()


def _get_conn():
    """Get a database connection"""
    return sqlite3.connect(DB_PATH)
