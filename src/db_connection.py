import os
import pyodbc
from dotenv import load_dotenv
import src.db_DAO as db_dao

load_dotenv()

# Global variable to store the current database connection ID
actual_db_id = -1

def build_connection_string(host: str, port: int, database: str, user: str, password: str) -> str:
    """
    Build SQL Server connection string with the provided parameters.

    Args:
        host: Database server host
        port: Database server port
        database: Database name
        user: Database username
        password: Database password

    Returns:
        Formatted connection string for SQL Server
    """
    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
        f"Connection Timeout=60;"
        f"Command Timeout=120;"
        f"Authentication=SqlPassword"
    )

def get_connection():
    """
    Get a database connection using the centralized connection logic.
    This function can be imported by both app.py and agent modules.
    """
    global actual_db_id

    try:
        # Get environment variables
        mssql_host = os.getenv('MSSQL_HOST')
        mssql_port = int(os.getenv('MSSQL_PORT', '1433'))
        mssql_user = os.getenv('MSSQL_USER')
        mssql_password = os.getenv('MSSQL_PASSWORD')
        mssql_db = os.getenv('MSSQL_DB')

        # Validate required environment variables
        if not all([mssql_host, mssql_user, mssql_password, mssql_db]):
            raise ValueError("Missing required MSSQL environment variables")

        # Step 1: Check if database connection exists in Database_connection table
        if actual_db_id == -1:
            actual_db_id = db_dao.exists_db(mssql_host, mssql_port, mssql_user)

        if actual_db_id != -1:
            # Step 2: Database connection exists, use its db_id
            db_connection = db_dao.get_db(actual_db_id)
            if db_connection:
                # Use the stored connection info
                conn_str = build_connection_string(
                    db_connection.db_host,
                    db_connection.db_port,
                    db_connection.db_name,
                    db_connection.db_user,
                    db_connection.db_password
                )
            else:
                raise ValueError("Failed to retrieve database connection even though the ID exists.")
        else:
            # Step 3: Database connection doesn't exist, create it
            new_db_connection = db_dao.DatabaseConnection(
                db_name=mssql_db,
                db_user=mssql_user,
                db_password=mssql_password,
                db_host=mssql_host,
                db_port=mssql_port
            )

            # Insert the new connection and get the db_id
            actual_db_id = db_dao.insert_db(new_db_connection)

            # Use environment variables for connection
            conn_str = build_connection_string(
                mssql_host,
                mssql_port,
                mssql_db,
                mssql_user,
                mssql_password
            )

        return pyodbc.connect(conn_str)

    except (pyodbc.Error, ValueError) as e:
        # If database connection management fails, fallback to direct environment connection
        print(f"Warning: Database connection management failed: {e}. Using direct environment connection.")
        actual_db_id = 1  # Use default id
        conn_str = build_connection_string(
            os.getenv('MSSQL_HOST'),
            int(os.getenv('MSSQL_PORT', 1433)),
            os.getenv('MSSQL_DB'),
            os.getenv('MSSQL_USER'),
            os.getenv('MSSQL_PASSWORD')
        )
        return pyodbc.connect(conn_str)

def get_actual_db_id():
    """Get the current actual_db_id value"""
    return actual_db_id

def set_actual_db_id(db_id: int):
    """Set the actual_db_id value"""
    global actual_db_id
    actual_db_id = db_id
