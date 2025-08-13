from typing import List, Optional, Tuple, Dict, Any, Union
from .connection_DAO import _ensure_db, _get_conn
from .models import (
    BlitzRecord, BlitzIndexRecord, BlitzCacheRecord,
    PROCEDURE_MODELS, PROCEDURE_TABLE_NAMES,
    PROCEDURE_CHAT_TABLE_NAMES, PROCEDURE_ID_FIELDS, COLUMN_MAPPING
)
import json

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
