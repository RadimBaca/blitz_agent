from typing import List, Optional, Tuple, Dict, Any, Union
from .connection_DAO import _ensure_db, _get_conn
from . import db_connection
from .models import (
    BlitzRecord, BlitzIndexRecord, BlitzCacheRecord,
    DBIndexRecord, Recommendation,
    PROCEDURE_MODELS, PROCEDURE_TABLE_NAMES,
    PROCEDURE_CHAT_TABLE_NAMES, PROCEDURE_ID_FIELDS, COLUMN_MAPPING,
    RECOMMENDATION_FK_MAPPING
)
import json
import datetime
import pyodbc
import sqlparse

# Initialize database on module import
_ensure_db()


def _map_raw_record_to_model(proc_name: str, raw_record: Dict[str, Any], procedure_order: int, pc_id: int):
    """Map raw database record to Pydantic model using column mapping"""
    model_class = PROCEDURE_MODELS[proc_name]
    column_map = COLUMN_MAPPING[proc_name]

    # Map raw columns to model field names
    mapped_data = {}
    for raw_col, model_field in column_map.items():
        if raw_col in raw_record:
            mapped_data[model_field] = raw_record[raw_col]

    # Add required fields
    mapped_data["procedure_order"] = procedure_order
    mapped_data["pc_id"] = pc_id

    # Store the entire raw record as JSON string for the raw_record field
    mapped_data["raw_record"] = json.dumps(raw_record, default=str)

    return model_class(**mapped_data)


def store_records(proc_name: str, records: List[Dict[str, Any]], db_id: int):
    """Store records in the appropriate procedure-specific table"""
    _ensure_db()
    conn = _get_conn()
    try:
        # Get p_id for proc_name
        cur = conn.execute("SELECT p_id FROM Procedure_type WHERE procedure_name = ?", (proc_name,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Procedure_type with procedure_name '{proc_name}' does not exist.")
        p_id = row[0]

        # Clean up existing data
        delete_chat_sessions(proc_name, db_id)
        delete_results(proc_name, db_id)
        conn.execute("DELETE FROM Procedure_call WHERE p_id = ? AND db_id = ?", (p_id, db_id))

        # Insert a new Procedure_call with db_id
        conn.execute("INSERT INTO Procedure_call (run, p_id, db_id) VALUES (datetime('now'), ?, ?)", (p_id, db_id))
        pc_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Get table name
        table_name = PROCEDURE_TABLE_NAMES[proc_name]

        # Insert records using Pydantic models for validation
        for i, raw_record in enumerate(records):
            # Create and validate the Pydantic model
            record_model = _map_raw_record_to_model(proc_name, raw_record, i, pc_id)

            # Convert model to dict for database insertion
            model_dict = record_model.model_dump(exclude={'_analyzed'})  # Exclude computed fields

            # Build INSERT statement from validated model data
            fields = []
            values = []
            for field, value in model_dict.items():
                if value is not None:  # Only insert non-None values
                    fields.append(field)
                    values.append(value)

            if fields:  # Only insert if we have data
                placeholders = ["?"] * len(fields)
                sql = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                conn.execute(sql, values)

        conn.commit()
    finally:
        conn.close()


def get_all_records(proc_name: str, db_id: int) -> List[Union[BlitzRecord, BlitzIndexRecord, BlitzCacheRecord]]:
    """Get all records for a procedure and database, returning Pydantic model instances"""
    _ensure_db()
    conn = _get_conn()
    try:
        # Validate that db_id exists
        cur = conn.execute("SELECT db_id FROM Database_connection WHERE db_id = ?", (db_id,))
        if not cur.fetchone():
            raise ValueError(f"Database connection with db_id '{db_id}' does not exist.")

        table_name = PROCEDURE_TABLE_NAMES[proc_name]
        id_field = PROCEDURE_ID_FIELDS[proc_name]
        chat_table = PROCEDURE_CHAT_TABLE_NAMES[proc_name]
        model_class = PROCEDURE_MODELS[proc_name]

        # Build SELECT query dynamically
        column_map = COLUMN_MAPPING[proc_name]
        select_fields = [f"r.{field}" for field in column_map.values()]
        select_fields.extend(["r.procedure_order", f"r.{id_field}"])

        cur = conn.execute(
            f"""
            SELECT {', '.join(select_fields)},
                (
                    SELECT COUNT(*) FROM {chat_table} WHERE {id_field} = r.{id_field}
                ) AS chat_count
            FROM Procedure_call pc
            JOIN Procedure_type pt ON pc.p_id = pt.p_id
            JOIN {table_name} r ON r.pc_id = pc.pc_id
            WHERE pt.procedure_name = ? AND pc.db_id = ?
            ORDER BY pc.run DESC, r.procedure_order ASC
            """, (proc_name, db_id)
        )

        records = []
        for row in cur.fetchall():
            # Build model data from database row
            model_data = {}

            # Map database fields to model fields
            for i, field in enumerate(column_map.values()):
                model_data[field] = row[i]

            # Add metadata fields
            procedure_order_idx = len(column_map)
            id_field_idx = len(column_map) + 1
            chat_count_idx = len(column_map) + 2

            model_data["procedure_order"] = row[procedure_order_idx]
            model_data["pc_id"] = 0  # Not needed for display, but required by model

            # Add the record ID field dynamically
            if proc_name == "sp_Blitz":
                model_data["pb_id"] = row[id_field_idx]
            elif proc_name == "sp_BlitzIndex":
                model_data["pbi_id"] = row[id_field_idx]
            elif proc_name == "sp_BlitzCache":
                model_data["pbc_id"] = row[id_field_idx]

            # Create Pydantic model instance
            record_model = model_class(**model_data)
            # Explicitly set the _analyzed field after model creation
            setattr(record_model, '_analyzed', row[chat_count_idx] > 0)
            records.append(record_model)

        return records
    finally:
        conn.close()


def get_record(proc_name: str, rec_id: int) -> Union[BlitzRecord, BlitzIndexRecord, BlitzCacheRecord]:
    """Get a specific record by procedure name and record ID, returning a Pydantic model instance"""
    _ensure_db()
    conn = _get_conn()
    try:
        table_name = PROCEDURE_TABLE_NAMES[proc_name]
        column_map = COLUMN_MAPPING[proc_name]
        model_class = PROCEDURE_MODELS[proc_name]
        id_field = PROCEDURE_ID_FIELDS[proc_name]
        select_fields = [f"r.{field}" for field in column_map.values()]
        select_fields.extend([f"r.{id_field}"])

        cur = conn.execute(
            f"""
            SELECT {', '.join(select_fields)}
            FROM Procedure_call pc
            JOIN Procedure_type pt ON pc.p_id = pt.p_id
            JOIN {table_name} r ON r.pc_id = pc.pc_id
            WHERE pt.procedure_name = ? AND r.procedure_order = ?
            ORDER BY pc.run DESC
            LIMIT 1
            """, (proc_name, rec_id)
        )
        row = cur.fetchone()
        if not row:
            raise IndexError("No record with this rec_id")

        # Build model data from database row
        model_data = {}
        for i, field in enumerate(column_map.values()):
            model_data[field] = row[i]

        # Add required fields
        model_data["procedure_order"] = rec_id
        model_data["pc_id"] = 0  # Not needed for display
        model_data["_analyzed"] = False  # Will be set separately if needed

        # Add the record ID field
        id_field_idx = len(column_map)
        if proc_name == "sp_Blitz":
            model_data["pb_id"] = row[id_field_idx]
        elif proc_name == "sp_BlitzIndex":
            model_data["pbi_id"] = row[id_field_idx]
        elif proc_name == "sp_BlitzCache":
            model_data["pbc_id"] = row[id_field_idx]

        # Create and return Pydantic model instance
        return model_class(**model_data)
    finally:
        conn.close()


def store_chat_history(proc_name: str, rec_id: int, chat_history: List[Tuple[str, str]]):
    """Store chat history for a specific record"""
    _ensure_db()
    conn = _get_conn()
    try:
        table_name = PROCEDURE_TABLE_NAMES[proc_name]
        chat_table = PROCEDURE_CHAT_TABLE_NAMES[proc_name]
        id_field = PROCEDURE_ID_FIELDS[proc_name]

        # Get the record ID for this proc_name and rec_id
        cur = conn.execute(
            f"""
            SELECT r.{id_field}
            FROM Procedure_call pc
            JOIN Procedure_type pt ON pc.p_id = pt.p_id
            JOIN {table_name} r ON r.pc_id = pc.pc_id
            WHERE pt.procedure_name = ? AND r.procedure_order = ?
            ORDER BY pc.run DESC
            LIMIT 1
            """, (proc_name, rec_id)
        )
        row = cur.fetchone()
        if not row:
            raise IndexError("No record with this rec_id")
        record_pk_id = row[0]

        # Remove previous chat for this record
        delete_chat_session_by_record_id(proc_name, record_pk_id)

        # Insert chat history as rows, one per tuple, preserving order
        for i, (role, msg) in enumerate(chat_history):
            conn.execute(
                f"INSERT INTO {chat_table} (response, type, chat_order, {id_field}) VALUES (?, ?, ?, ?)",
                (msg, role, i, record_pk_id)
            )
        conn.commit()
    finally:
        conn.close()


def get_chat_history(proc_name: str, rec_id: int) -> Optional[List[Tuple[str, str]]]:
    """Get chat history for a specific record"""
    _ensure_db()
    conn = _get_conn()
    try:
        table_name = PROCEDURE_TABLE_NAMES[proc_name]
        chat_table = PROCEDURE_CHAT_TABLE_NAMES[proc_name]
        id_field = PROCEDURE_ID_FIELDS[proc_name]

        cur = conn.execute(
            f"""
            SELECT c.type, c.response
            FROM Procedure_call pc
            JOIN Procedure_type pt ON pc.p_id = pt.p_id
            JOIN {table_name} r ON r.pc_id = pc.pc_id
            JOIN {chat_table} c ON c.{id_field} = r.{id_field}
            WHERE pt.procedure_name = ? AND r.procedure_order = ?
            ORDER BY pc.run DESC, c.chat_order ASC
            """, (proc_name, rec_id)
        )
        chat = list(cur.fetchall())
        if not chat:
            return None
        return chat
    finally:
        conn.close()


def clear_all(db_id: int):
    """Clear all data for a specific database ID"""
    _ensure_db()
    conn = _get_conn()
    try:
        # Clear chat sessions for all procedure types
        for proc_name in PROCEDURE_MODELS.keys():
            delete_chat_sessions(proc_name, db_id)
            delete_results(proc_name, db_id)

        # Delete procedure calls for this db_id
        conn.execute("DELETE FROM Procedure_call WHERE db_id = ?", (db_id,))
        conn.commit()
    finally:
        conn.close()


def delete_results(proc_name: str, db_id: int):
    """Delete results for a specific procedure and database"""
    _ensure_db()
    conn = _get_conn()
    try:
        table_name = PROCEDURE_TABLE_NAMES[proc_name]
        id_field = PROCEDURE_ID_FIELDS[proc_name]
        recommendation_fk_field = RECOMMENDATION_FK_MAPPING[proc_name]

        # First delete related recommendations
        conn.execute(f"""
            DELETE FROM Recommendation
            WHERE {recommendation_fk_field} IN (
                SELECT r.{id_field} FROM Procedure_call pc
                JOIN Procedure_type pt ON pc.p_id = pt.p_id
                JOIN {table_name} r ON r.pc_id = pc.pc_id
                WHERE pt.procedure_name = ? AND pc.db_id = ?
            )
        """, (proc_name, db_id))

        # Then delete the main procedure records
        conn.execute(f"""
            DELETE FROM {table_name}
            WHERE pc_id IN (
                SELECT pc.pc_id FROM Procedure_call pc
                JOIN Procedure_type pt ON pc.p_id = pt.p_id
                WHERE pt.procedure_name = ? AND pc.db_id = ?
            )
        """, (proc_name, db_id))
        conn.commit()
    finally:
        conn.close()


def delete_chat_sessions(proc_name: str, db_id: int):
    """Delete chat sessions for a specific procedure and database"""
    _ensure_db()
    conn = _get_conn()
    try:
        table_name = PROCEDURE_TABLE_NAMES[proc_name]
        chat_table = PROCEDURE_CHAT_TABLE_NAMES[proc_name]
        id_field = PROCEDURE_ID_FIELDS[proc_name]

        conn.execute(f"""
            DELETE FROM {chat_table}
            WHERE {id_field} IN (
                SELECT r.{id_field} FROM Procedure_call pc
                JOIN Procedure_type pt ON pc.p_id = pt.p_id
                JOIN {table_name} r ON r.pc_id = pc.pc_id
                WHERE pt.procedure_name = ? AND pc.db_id = ?
            )
        """, (proc_name, db_id))
        conn.commit()
    finally:
        conn.close()


def delete_chat_session_by_record_id(proc_name: str, record_pk_id: int):
    """Delete chat session for a specific record ID"""
    _ensure_db()
    conn = _get_conn()
    try:
        chat_table = PROCEDURE_CHAT_TABLE_NAMES[proc_name]
        id_field = PROCEDURE_ID_FIELDS[proc_name]

        conn.execute(f"""
            DELETE FROM {chat_table}
            WHERE {id_field} = ?
        """, (record_pk_id,))
        conn.commit()
    finally:
        conn.close()


def store_db_indexes(indexes_data: List[DBIndexRecord], pbi_id: int):
    """Store DB_Indexes data for a specific BlitzIndex record"""
    _ensure_db()
    conn = _get_conn()
    try:
        # First delete any existing indexes for this pbi_id
        conn.execute("DELETE FROM DB_Indexes WHERE pbi_id = ?", (pbi_id,))

        # Insert new index records
        for index_record in indexes_data:
            # Ensure pbi_id is set correctly
            index_record.pbi_id = pbi_id

            # Convert model to dict for database insertion
            model_dict = index_record.model_dump()

            # Build INSERT statement
            fields = []
            values = []
            for field, value in model_dict.items():
                if value is not None:  # Only insert non-None values
                    fields.append(field)
                    values.append(value)

            if fields:  # Only insert if we have data
                placeholders = ["?"] * len(fields)
                sql = f"INSERT INTO DB_Indexes ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                conn.execute(sql, values)

        conn.commit()
    finally:
        conn.close()


def get_db_indexes(pbi_id: int) -> List[DBIndexRecord]:
    """Get all DB_Indexes records for a specific BlitzIndex record"""
    _ensure_db()
    conn = _get_conn()
    try:
        cur = conn.execute("""
            SELECT di_id, pbi_id, db_schema_object_indexid, index_definition, secret_columns,
                   fill_factor, index_usage_summary, index_op_stats, index_size_summary,
                   partition_compression_detail, index_lock_wait_summary, is_referenced_by_foreign_key,
                   fks_covered_by_index, last_user_seek, last_user_scan, last_user_lookup,
                   last_user_update, create_date, modify_date, page_latch_wait_count,
                   page_latch_wait_time, page_io_latch_wait_count, page_io_latch_wait_time,
                   create_tsql, drop_tsql
            FROM DB_Indexes
            WHERE pbi_id = ?
            ORDER BY di_id
        """, (pbi_id,))

        records = []
        for row in cur.fetchall():
            record_data = {
                'di_id': row[0],
                'pbi_id': row[1],
                'db_schema_object_indexid': row[2],
                'index_definition': row[3],
                'secret_columns': row[4],
                'fill_factor': row[5],
                'index_usage_summary': row[6],
                'index_op_stats': row[7],
                'index_size_summary': row[8],
                'partition_compression_detail': row[9],
                'index_lock_wait_summary': row[10],
                'is_referenced_by_foreign_key': row[11],
                'fks_covered_by_index': row[12],
                'last_user_seek': row[13],
                'last_user_scan': row[14],
                'last_user_lookup': row[15],
                'last_user_update': row[16],
                'create_date': row[17],
                'modify_date': row[18],
                'page_latch_wait_count': row[19],
                'page_latch_wait_time': row[20],
                'page_io_latch_wait_count': row[21],
                'page_io_latch_wait_time': row[22],
                'create_tsql': row[23],
                'drop_tsql': row[24]
            }
            records.append(DBIndexRecord(**record_data))

        return records
    finally:
        conn.close()


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


def process_over_indexing_analysis(record: BlitzIndexRecord) -> List[DBIndexRecord]:
    """
    Process over-indexing analysis for BlitzIndex records by executing the more_info SQL
    and storing the detailed index data.

    Args:
        record: The BlitzIndexRecord containing the more_info SQL to execute
        db_connection: Database connection object

    Returns:
        List of DBIndexRecord objects containing the processed index data

    Raises:
        pyodbc.Error: If database operation fails
        ValueError: If data processing fails
        KeyError: If required columns are missing
    """
    index_records = []

    # Use SQL Server connection for executing more_info SQL
    try:
        with db_connection.get_connection() as sql_server_conn:
            cursor = sql_server_conn.cursor()
            cursor.execute(record.more_info)

            # Skip to the result set with index data
            while cursor.description is None:
                if not cursor.nextset():
                    break

        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

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
                index_record = DBIndexRecord(pbi_id=record.pbi_id, **mapped_data)
                index_records.append(index_record)

            # Store the detailed index data
            if index_records:
                store_db_indexes(index_records, record.pbi_id)

    except (pyodbc.Error, ValueError, KeyError) as e:
        raise e

    return index_records


# Recommendation methods
def insert_recommendation(description: str, sql_command: Optional[str],
                         pb_id: Optional[int] = None,
                         pbi_id: Optional[int] = None,
                         pbc_id: Optional[int] = None) -> int:
    """Insert a new recommendation and return its ID"""
    _ensure_db()
    conn = _get_conn()

    # Validate that exactly one foreign key is provided
    foreign_keys = [pb_id, pbi_id, pbc_id]
    non_null_keys = [key for key in foreign_keys if key is not None]

    if len(non_null_keys) != 1:
        raise ValueError("Exactly one of pb_id, pbi_id, or pbc_id must be provided")

    try:
        cur = conn.execute("""
            INSERT INTO Recommendation (description, sql_command, pb_id, pbi_id, pbc_id)
            VALUES (?, ?, ?, ?, ?)
        """, (description, sql_command, pb_id, pbi_id, pbc_id))

        # Get the last inserted ID
        cur = conn.execute("SELECT last_insert_rowid()")
        recommendation_id = cur.fetchone()[0]

        conn.commit()
        return recommendation_id

    except pyodbc.Error as e:
        conn.rollback()
        raise e


def get_recommendations(db_id: int, procedure: str) -> List[Recommendation]:
    """Get all recommendations for a specific procedure and database"""
    _ensure_db()
    conn = _get_conn()

    # Map procedure names to table columns and foreign key fields
    procedure_mapping = {
        "sp_Blitz": ("pb_id", "Procedure_blitz"),
        "sp_BlitzIndex": ("pbi_id", "Procedure_blitzindex"),
        "sp_BlitzCache": ("pbc_id", "Procedure_blitzcache")
    }

    if procedure not in procedure_mapping:
        raise ValueError(f"Unsupported procedure: {procedure}")

    fk_field, procedure_table = procedure_mapping[procedure]

    try:
        # Get procedure calls for this database
        cur = conn.execute("""
            SELECT pc_id FROM Procedure_call
            WHERE db_id = ? AND p_id = (
                SELECT p_id FROM Procedure_type WHERE procedure_name = ?
            )
        """, (db_id, procedure))

        pc_ids = [row[0] for row in cur.fetchall()]

        if not pc_ids:
            return []

        # Create placeholders for IN clause
        placeholders = ','.join(['?'] * len(pc_ids))

        # Get recommendations for this procedure and database
        query = f"""
            SELECT r.id_recom, r.description, r.sql_command,
                   r.pb_id, r.pbi_id, r.pbc_id, r.created_at
            FROM Recommendation r
            JOIN {procedure_table} p ON r.{fk_field} = p.{fk_field}
            WHERE p.pc_id IN ({placeholders})
            ORDER BY r.created_at DESC
        """

        cur = conn.execute(query, pc_ids)
        recommendations = []

        for row in cur.fetchall():
            recommendation = Recommendation(
                id_recom=row[0],
                description=row[1],
                sql_command=row[2],
                pb_id=row[3],
                pbi_id=row[4],
                pbc_id=row[5],
                created_at=row[6]
            )
            recommendations.append(recommendation)

        return recommendations

    except pyodbc.Error as e:
        raise e


def get_all_recommendations(db_id: int) -> List[Recommendation]:
    """Get all recommendations for a specific database across all procedures"""
    _ensure_db()
    conn = _get_conn()

    try:
        # Get procedure calls for this database
        cur = conn.execute("SELECT pc_id FROM Procedure_call WHERE db_id = ?", (db_id,))
        pc_ids = [row[0] for row in cur.fetchall()]

        if not pc_ids:
            return []

        # Create placeholders for IN clause
        placeholders = ','.join(['?'] * len(pc_ids))

        # Get all recommendations for this database
        query = f"""
            SELECT DISTINCT r.id_recom, r.description, r.sql_command,
                   r.pb_id, r.pbi_id, r.pbc_id, r.created_at
            FROM Recommendation r
            LEFT JOIN Procedure_blitz pb ON r.pb_id = pb.pb_id
            LEFT JOIN Procedure_blitzindex pbi ON r.pbi_id = pbi.pbi_id
            LEFT JOIN Procedure_blitzcache pbc ON r.pbc_id = pbc.pbc_id
            WHERE pb.pc_id IN ({placeholders})
               OR pbi.pc_id IN ({placeholders})
               OR pbc.pc_id IN ({placeholders})
            ORDER BY r.created_at DESC
        """

        # Triple the pc_ids for the three conditions
        params = pc_ids + pc_ids + pc_ids
        cur = conn.execute(query, params)

        recommendations = []
        for row in cur.fetchall():
            recommendation = Recommendation(
                id_recom=row[0],
                description=row[1],
                sql_command=row[2],
                pb_id=row[3],
                pbi_id=row[4],
                pbc_id=row[5],
                created_at=row[6]
            )
            recommendations.append(recommendation)

        return recommendations

    except pyodbc.Error as e:
        raise e


def get_recommendation(db_id: int, id_recom: int) -> Optional[Recommendation]:
    """Get a specific recommendation by ID for a database"""
    _ensure_db()
    conn = _get_conn()

    try:
        # Get procedure calls for this database
        cur = conn.execute("SELECT pc_id FROM Procedure_call WHERE db_id = ?", (db_id,))
        pc_ids = [row[0] for row in cur.fetchall()]

        if not pc_ids:
            return None

        # Create placeholders for IN clause
        placeholders = ','.join(['?'] * len(pc_ids))

        # Get the specific recommendation
        query = f"""
            SELECT r.id_recom, r.description, r.sql_command,
                   r.pb_id, r.pbi_id, r.pbc_id, r.created_at
            FROM Recommendation r
            LEFT JOIN Procedure_blitz pb ON r.pb_id = pb.pb_id
            LEFT JOIN Procedure_blitzindex pbi ON r.pbi_id = pbi.pbi_id
            LEFT JOIN Procedure_blitzcache pbc ON r.pbc_id = pbc.pbc_id
            WHERE r.id_recom = ?
              AND (pb.pc_id IN ({placeholders})
                   OR pbi.pc_id IN ({placeholders})
                   OR pbc.pc_id IN ({placeholders}))
        """

        # Create parameters: id_recom + triple pc_ids
        params = [id_recom] + pc_ids + pc_ids + pc_ids
        cur = conn.execute(query, params)

        row = cur.fetchone()
        if row:
            return Recommendation(
                id_recom=row[0],
                description=row[1],
                sql_command=row[2],
                pb_id=row[3],
                pbi_id=row[4],
                pbc_id=row[5],
                created_at=row[6]
            )

        return None

    except pyodbc.Error as e:
        raise e


def get_recommendations_for_record(procedure_name: str, record_id: int) -> List[Recommendation]:
    """Get all recommendations for a specific record"""
    _ensure_db()
    conn = _get_conn()

    # Map procedure names to foreign key fields
    fk_mapping = {
        "sp_Blitz": "pb_id",
        "sp_BlitzIndex": "pbi_id",
        "sp_BlitzCache": "pbc_id"
    }

    if procedure_name not in fk_mapping:
        raise ValueError(f"Unsupported procedure: {procedure_name}")

    fk_field = fk_mapping[procedure_name]

    try:
        query = f"""
            SELECT id_recom, description, sql_command, pb_id, pbi_id, pbc_id, created_at
            FROM Recommendation
            WHERE {fk_field} = ?
            ORDER BY created_at DESC
        """

        cur = conn.execute(query, (record_id,))
        recommendations = []

        for row in cur.fetchall():
            recommendation = Recommendation(
                id_recom=row[0],
                description=row[1],
                sql_command=row[2],
                pb_id=row[3],
                pbi_id=row[4],
                pbc_id=row[5],
                created_at=row[6]
            )
            recommendations.append(recommendation)

        return recommendations

    except pyodbc.Error as e:
        raise e
