import os
import pyodbc
from dotenv import load_dotenv
import datetime
import sqlparse

import src.db_DAO as db_dao
import src.models as models

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


def probe_db_info(host: str, port: int, database: str, user: str, password: str):
    """
    Connect to the target SQL Server and attempt to read server version and instance memory (MB).
    Returns a tuple (version: Optional[str], instance_memory_mb: Optional[int]).
    """
    conn_str = build_connection_string(host, port, database, user, password)
    version = None
    instance_memory_mb = None
    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cur = conn.cursor()
            try:
                cur.execute("SELECT @@VERSION")
                row = cur.fetchone()
                if row:
                    version = str(row[0])
            except Exception:
                version = None

            # Try a few ways to get instance memory in MB; permissions may vary
            try:
                cur.execute("SELECT total_physical_memory_kb FROM sys.dm_os_sys_memory")
                row = cur.fetchone()
                if row and row[0] is not None:
                    instance_memory_mb = int(row[0]) // 1024
            except Exception:
                try:
                    cur.execute("SELECT TOP 1 cntr_value/1024 FROM sys.dm_os_performance_counters WHERE counter_name = 'Total Server Memory (KB)'")
                    row = cur.fetchone()
                    if row and row[0] is not None:
                        instance_memory_mb = int(row[0])
                except Exception:
                    instance_memory_mb = None
    except Exception:
        return None, None

    return version, instance_memory_mb

def check_blitz_procedures(host: str, port: int, database: str, user: str, password: str) -> bool:
    """
    Check if Blitz procedures (sp_Blitz, sp_BlitzIndex, sp_BlitzCache) exist in the database.

    Args:
        host: Database server host
        port: Database server port
        database: Database name
        user: Database username
        password: Database password

    Returns:
        True if at least one Blitz procedure exists, False otherwise, None if connection fails
    """
    try:
        conn_str = build_connection_string(host, port, database, user, password)
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM sys.objects WHERE type = 'P' AND name IN ('sp_Blitz', 'sp_BlitzIndex', 'sp_BlitzCache')")
            row = cur.fetchone()
            return row and row[0] > 0
    except Exception:
        return None

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
            temp_conn_str = build_connection_string(
                mssql_host,
                mssql_port,
                mssql_db,
                mssql_user,
                mssql_password
            )
            blitz_exists = check_blitz_procedures(mssql_host, mssql_port, mssql_db, mssql_user, mssql_password)

            new_db_connection = db_dao.DatabaseConnection(
                db_name=mssql_db,
                db_user=mssql_user,
                db_password=mssql_password,
                db_host=mssql_host,
                db_port=mssql_port,
                has_blitz_procedures=blitz_exists
            )

            # Probe database for server metadata and attach if available
            ver, mem_mb = probe_db_info(mssql_host, mssql_port, mssql_db, mssql_user, mssql_password)
            new_db_connection.version = ver
            new_db_connection.instance_memory_mb = mem_mb

            # Insert the new connection and get the db_id
            actual_db_id = db_dao.insert_db(new_db_connection)

            # Use environment variables for connection
            conn_str = temp_conn_str

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

def get_actual_db_name():
    """Get the name of the currently connected database"""
    db_connection = db_dao.get_db(actual_db_id)
    if db_connection:
        return db_connection.db_name
    return None


def safe_pretty_json(record: dict) -> dict:
    """Convert record values to safe JSON-serializable format"""
    safe_record = {}
    for k, v in record.items():
        if k == "Query Text":
            safe_record[k] = sqlparse.format(v, keyword_case='upper', output_format='sql', reindent=True)
        elif isinstance(v, datetime.datetime):
            safe_record[k] = v.isoformat()
        elif isinstance(v, datetime.date):
            safe_record[k] = v.isoformat()
        elif isinstance(v, datetime.time):
            safe_record[k] = v.isoformat()
        elif isinstance(v, bytes):
            # Convert bytes to hex string for display
            safe_record[k] = v.hex() if v else ''
        else:
            safe_record[k] = v
    return safe_record

def exec_more_info(record, index_records, finding_records):
    with get_connection() as sql_server_conn:
        cursor = sql_server_conn.cursor()
        cursor.execute(record.more_info)

            # Skip to the result set with index data
        while cursor.description is None:
            if not cursor.nextset():
                break

            # Process first result set (Q1 - Index details)
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

                # print(f"Fetched {len(rows)} rows from sp_BlitzIndex output")
                # Convert rows to list of DBIndexRecord objects and skip first row (Q1)
            for i, row in enumerate(rows):
                if i == 0:  # Skip first row (Q1)
                    continue
                row_dict = dict(zip(columns, row))
                    # Serialize the data for storage
                serialized_row = safe_pretty_json(row_dict)

                    # Map sp_BlitzIndex columns to DBIndexRecord fields
                mapped_data = {}

                    # Map the columns from sp_BlitzIndex to DBIndexRecord fields
                column_mapping = {
                        'Details: db_schema.table.index(indexid)': 'db_schema_object_indexid',
                        'Definition: [Property] ColumnName {datatype maxbytes}': 'index_definition',
                        'Secret Columns': 'secret_columns',
                        'Fillfactor': 'fill_factor',
                        'Usage Stats': 'index_usage_summary',
                        'Op Stats': 'index_op_stats',
                        'Size': 'index_size_summary',
                        'Compression Type': 'partition_compression_detail',
                        'Lock Waits': 'index_lock_wait_summary',
                        'Referenced by FK?': 'is_referenced_by_foreign_key',
                        'FK Covered by Index?': 'fks_covered_by_index',
                        'Last User Seek': 'last_user_seek',
                        'Last User Scan': 'last_user_scan',
                        'Last User Lookup': 'last_user_lookup',
                        'Last User Write': 'last_user_update',
                        'Created': 'create_date',
                        'Last Modified': 'modify_date',
                        'Page Latch Wait Count': 'page_latch_wait_count',
                        'Page Latch Wait Time (D:H:M:S)': 'page_latch_wait_time',
                        'Page IO Latch Wait Count': 'page_io_latch_wait_count',
                        'Page IO Latch Wait Time (D:H:M:S)': 'page_io_latch_wait_time',
                        'Create TSQL': 'create_tsql',
                        'Drop TSQL': 'drop_tsql'
                    }

                for sp_column, db_field in column_mapping.items():
                    if sp_column in serialized_row:
                        value = serialized_row[sp_column]
                            # Convert boolean strings to integers for FK fields
                        if db_field == 'is_referenced_by_foreign_key' and isinstance(value, str):
                            mapped_data[db_field] = 1 if value.lower() == 'true' else 0
                        else:
                            mapped_data[db_field] = value

                    # Create DBIndexRecord object with mapped data
                index_record = models.DBIndexRecord(pbi_id=record.pbi_id, **mapped_data)
                index_records.append(index_record)

            # Process second result set (Q2 - Missing index findings)
        if cursor.nextset() and cursor.description:
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

                # Convert rows to list of DBFindingRecord objects
            for row in rows:
                row_dict = dict(zip(columns, row))

                    # Map columns from Q2 to DBFindingRecord fields
                finding_data = {
                        'pbi_id': record.pbi_id,
                        'finding': row_dict.get('Finding'),
                        'url': row_dict.get('URL'),
                        'estimated_benefit': row_dict.get('Estimated Benefit'),
                        'missing_index_request': row_dict.get('Missing Index Request'),
                        'estimated_impact': row_dict.get('Estimated Impact'),
                        'create_tsql': row_dict.get('Create TSQL'),
                        'sample_query_plan': row_dict.get('Sample Query Plan')
                    }

                    # Create DBFindingRecord object
                finding_record = models.DBFindingRecord(**finding_data)
                finding_records.append(finding_record)



def exec_blitz(procedure_name):
    records = []

    # Get database name for procedures that require it
    db_connection = db_dao.get_db(get_actual_db_id())
    database_name = db_connection.db_name if db_connection else None

    with get_connection() as db_connection:
        cursor = db_connection.cursor()

            # Add @DatabaseName parameter for sp_BlitzCache and sp_BlitzIndex
        if procedure_name == 'sp_BlitzCache' and database_name:
            cursor.execute(f"EXEC {procedure_name} @DatabaseName = ?", (database_name,))
        elif procedure_name == 'sp_BlitzIndex' and database_name:
            cursor.execute(f"EXEC {procedure_name} @IncludeInactiveIndexes=1, @Mode=4, @DatabaseName = ?", (database_name,))
        else:
            cursor.execute(f"EXEC {procedure_name}")

        while cursor.description is None:
            if not cursor.nextset():
                return False, []

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        records = []
        for row in rows:
            full = dict(zip(columns, row))
                # Apply serialization for storage
            serialized_full = safe_pretty_json(full)
                # Store the original SQL Server data for the DAO
            records.append(serialized_full)
    return True, records
