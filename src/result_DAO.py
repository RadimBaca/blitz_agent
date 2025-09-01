from typing import List, Optional, Tuple, Dict, Any, Union
from .connection_DAO import _ensure_db, get_conn_ctx
from . import db_connection
from .models import (
    BlitzRecord, BlitzIndexRecord, BlitzCacheRecord,
    DBIndexRecord, DBFindingRecord, Recommendation,
    PROCEDURE_MODELS, PROCEDURE_TABLE_NAMES,
    PROCEDURE_CHAT_TABLE_NAMES, PROCEDURE_ID_FIELDS, COLUMN_MAPPING,
    RECOMMENDATION_FK_MAPPING
)
import json
import pyodbc
import re
import contextlib as lcontext
import logging as l

# Initialize database on module import
_ensure_db()

# module logger
logger = l.getLogger(__name__)

def _row_to_dict(cur, row) -> Dict[str, Any]:
    """Convert a DB cursor row to a dict using cursor.description for column names."""
    return dict(zip([col[0] for col in cur.description], row))


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


def store_records(proc_name: str, records: List[Dict[str, Any]], db_id: int) -> None:
    """Store records in the appropriate procedure-specific table"""
    with get_conn_ctx() as conn:
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
        conn.execute("INSERT INTO Procedure_call (run, p_id, db_id) VALUES (datetime('now'), ?, ?)", (p_id, db_id))
        pc_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        db_table_name = PROCEDURE_TABLE_NAMES[proc_name]

        # Insert records using Pydantic models for validation
        for i, raw_record in enumerate(records):
            # Create and validate the Pydantic model
            record_model = _map_raw_record_to_model(proc_name, raw_record, i, pc_id)

            # For BlitzIndex records, extract parameters from EXEC command
            if proc_name == "sp_BlitzIndex" and hasattr(record_model, 'more_info') and record_model.more_info:
                database_name, schema_name, table_name = extract_exec_parameters(record_model.more_info)
                if any([database_name, schema_name, table_name]):
                    record_model.database_name = database_name
                    record_model.schema_name = schema_name
                    record_model.table_name = table_name

            # Convert model to dict for database insertion
            model_dict = record_model.model_dump(exclude={'_analyzed'})  # Exclude computed fields

            # Convert boolean values to integers for SQLite compatibility
            for field, value in list(model_dict.items()):
                if isinstance(value, bool):
                    model_dict[field] = int(value)

            # Build INSERT statement from validated model data
            fields = []
            values = []
            for field, value in model_dict.items():
                if value is not None:  # Only insert non-None values
                    fields.append(field)
                    values.append(value)

            if fields:  # Only insert if we have data
                placeholders = ["?"] * len(fields)
                sql = f"INSERT INTO {db_table_name} ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                conn.execute(sql, values)


def get_all_records(proc_name: str, db_id: int) -> List[Union[BlitzRecord, BlitzIndexRecord, BlitzCacheRecord]]:
    """Get all records for a procedure and database, returning Pydantic model instances"""
    with get_conn_ctx() as conn:
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

        # Add special fields for BlitzIndex
        if proc_name == "sp_BlitzIndex":
            select_fields.extend(["r.database_name", "r.schema_name", "r.table_name", "r.index_findings_loaded"])

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

        records: List[Union[BlitzRecord, BlitzIndexRecord, BlitzCacheRecord]] = []
        for row in cur.fetchall():
            row_dict = _row_to_dict(cur, row)

            # Map selected fields to model field names
            model_data: Dict[str, Any] = {}
            for raw_field_name in column_map.values():
                model_data[raw_field_name] = row_dict.get(raw_field_name)

            # Add metadata
            model_data["procedure_order"] = row_dict.get("procedure_order")
            model_data["pc_id"] = 0

            # Add the record ID field dynamically and BlitzIndex special fields
            if proc_name == "sp_Blitz":
                model_data["pb_id"] = row_dict.get(id_field)
            elif proc_name == "sp_BlitzIndex":
                model_data["pbi_id"] = row_dict.get(id_field)
                model_data["database_name"] = row_dict.get("database_name")
                model_data["schema_name"] = row_dict.get("schema_name")
                model_data["table_name"] = row_dict.get("table_name")
                model_data["index_findings_loaded"] = bool(row_dict.get("index_findings_loaded")) if row_dict.get("index_findings_loaded") is not None else False
            elif proc_name == "sp_BlitzCache":
                model_data["pbc_id"] = row_dict.get(id_field)

            # Create Pydantic model instance
            record_model = model_class(**model_data)
            setattr(record_model, '_analyzed', row_dict.get('chat_count', 0) > 0)
            records.append(record_model)

        return records


def get_record(proc_name: str, procedure_order: int, db_id: int) -> Union[BlitzRecord, BlitzIndexRecord, BlitzCacheRecord]:
    """Get a specific record by procedure name and record ID, returning a Pydantic model instance"""
    with get_conn_ctx() as conn:
        table_name = PROCEDURE_TABLE_NAMES[proc_name]
        column_map = COLUMN_MAPPING[proc_name]
        model_class = PROCEDURE_MODELS[proc_name]
        id_field = PROCEDURE_ID_FIELDS[proc_name]
        select_fields = [f"r.{field}" for field in column_map.values()]
        select_fields.extend([f"r.{id_field}"])

        # Add special fields for BlitzIndex
        if proc_name == "sp_BlitzIndex":
            select_fields.extend(["r.database_name", "r.schema_name", "r.table_name", "r.index_findings_loaded"])

        cur = conn.execute(
            f"""
            SELECT {', '.join(select_fields)}
            FROM Procedure_call pc
            JOIN Procedure_type pt ON pc.p_id = pt.p_id
            JOIN {table_name} r ON r.pc_id = pc.pc_id
            WHERE pt.procedure_name = ? AND r.procedure_order = ? AND db_id = ?
            ORDER BY pc.run DESC
            LIMIT 1
            """, (proc_name, procedure_order, db_id)
        )
        row = cur.fetchone()
        if not row:
            raise IndexError("No record with this rec_id")

        row_dict = _row_to_dict(cur, row)

        # Map database fields to model fields
        model_data: Dict[str, Any] = {}
        for raw_field_name in column_map.values():
            model_data[raw_field_name] = row_dict.get(raw_field_name)

        # Add required metadata
        model_data["procedure_order"] = procedure_order
        model_data["pc_id"] = 0
        model_data["_analyzed"] = False

        # Add the record ID and special BlitzIndex fields
        if proc_name == "sp_Blitz":
            model_data["pb_id"] = row_dict.get(id_field)
        elif proc_name == "sp_BlitzIndex":
            model_data["pbi_id"] = row_dict.get(id_field)
            model_data["database_name"] = row_dict.get("database_name")
            model_data["schema_name"] = row_dict.get("schema_name")
            model_data["table_name"] = row_dict.get("table_name")
            model_data["index_findings_loaded"] = bool(row_dict.get("index_findings_loaded")) if row_dict.get("index_findings_loaded") is not None else False
        elif proc_name == "sp_BlitzCache":
            model_data["pbc_id"] = row_dict.get(id_field)

        return model_class(**model_data)


def store_chat_history(proc_name: str, rec_id: int, chat_history: List[Tuple[str, str]]) -> None:
    """Store chat history for a specific record"""
    with get_conn_ctx() as conn:
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


def get_chat_history(proc_name: str, rec_id: int) -> Optional[List[Tuple[str, str]]]:
    """Get chat history for a specific record"""
    with get_conn_ctx() as conn:
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


def clear_all(db_id: int) -> None:
    """Clear all data for a specific database ID"""
    with get_conn_ctx() as conn:
        # Clear chat sessions for all procedure types
        for proc_name in PROCEDURE_MODELS.keys():
            delete_chat_sessions(proc_name, db_id)
            delete_results(proc_name, db_id)

        conn.execute("DELETE FROM Procedure_call WHERE db_id = ?", (db_id,))


def delete_results(proc_name: str, db_id: int) -> None:
    """Delete results for a specific procedure and database"""
    with get_conn_ctx() as conn:
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



def delete_chat_sessions(proc_name: str, db_id: int) -> None:
    """Delete chat sessions for a specific procedure and database"""
    with get_conn_ctx() as conn:
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


def delete_chat_session_by_record_id(proc_name: str, record_pk_id: int) -> None:
    """Delete chat session for a specific record ID"""
    with get_conn_ctx() as conn:
        chat_table = PROCEDURE_CHAT_TABLE_NAMES[proc_name]
        id_field = PROCEDURE_ID_FIELDS[proc_name]

        conn.execute(f"""
            DELETE FROM {chat_table}
            WHERE {id_field} = ?
        """, (record_pk_id,))
        conn.commit()


def process_more_info(record: BlitzIndexRecord) -> Tuple[List[DBIndexRecord], List[DBFindingRecord]]:
    """
    Process over-indexing analysis for BlitzIndex records by executing the more_info SQL
    and storing the detailed index data and findings.

    Args:
        record: The BlitzIndexRecord containing the more_info SQL to execute

    Returns:
        Tuple of (List of DBIndexRecord objects, List of DBFindingRecord objects)

    Raises:
        pyodbc.Error: If database operation fails
        ValueError: If data processing fails
        KeyError: If required columns are missing
    """
    index_records = []
    finding_records = []

    # Use SQL Server connection for executing more_info SQL
    try:
        db_connection.exec_more_info(record, index_records, finding_records)
    except (pyodbc.Error, ValueError, KeyError):
        logger.exception("exec_more_info failed for pbi_id=%s", getattr(record, 'pbi_id', None))
        raise

    logger.info("Processed %d index records and %d findings for PBI ID %s",
                len(index_records), len(finding_records), getattr(record, 'pbi_id', None))
    # Store the data
    if index_records:
        store_db_indexes_for_record(record.pbi_id, [r.model_dump() for r in index_records])

    if finding_records:
        store_db_findings_for_record(record.pbi_id, [r.model_dump() for r in finding_records])

    # Mark as loaded
    mark_index_findings_loaded(record.pbi_id)

    return index_records, finding_records


# Recommendation methods
def insert_recommendation(description: str, sql_command: Optional[str],
                         pb_id: Optional[int] = None,
                         pbi_id: Optional[int] = None,
                         pbc_id: Optional[int] = None) -> int:
    """Insert a new recommendation and return its ID"""
    # Validate that exactly one foreign key is provided
    foreign_keys = [pb_id, pbi_id, pbc_id]
    non_null_keys = [key for key in foreign_keys if key is not None]

    if len(non_null_keys) != 1:
        raise ValueError("Exactly one of pb_id, pbi_id, or pbc_id must be provided")

    with get_conn_ctx() as conn:
        try:
            conn.execute("""
                INSERT INTO Recommendation (description, sql_command, pb_id, pbi_id, pbc_id)
                VALUES (?, ?, ?, ?, ?)
            """, (description, sql_command, pb_id, pbi_id, pbc_id))

            # Get the last inserted ID
            cur = conn.execute("SELECT last_insert_rowid()")
            recommendation_id = cur.fetchone()[0]

            return int(recommendation_id)
        except pyodbc.Error:
            conn.rollback()
            logger.exception("Failed to insert recommendation")
            raise


def get_recommendations(db_id: int, procedure: str) -> List[Recommendation]:
    """Get all recommendations for a specific procedure and database"""
    # Map procedure names to table columns and foreign key fields
    procedure_mapping = {
        "sp_Blitz": ("pb_id", "Procedure_blitz"),
        "sp_BlitzIndex": ("pbi_id", "Procedure_blitzindex"),
        "sp_BlitzCache": ("pbc_id", "Procedure_blitzcache")
    }

    if procedure not in procedure_mapping:
        raise ValueError(f"Unsupported procedure: {procedure}")

    fk_field, procedure_table = procedure_mapping[procedure]

    with get_conn_ctx() as conn:
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
        recommendations: List[Recommendation] = []

        for row in cur.fetchall():
            recommendations.append(Recommendation(
                id_recom=row[0],
                description=row[1],
                sql_command=row[2],
                pb_id=row[3],
                pbi_id=row[4],
                pbc_id=row[5],
                created_at=row[6]
            ))

        return recommendations


def get_all_recommendations(db_id: int) -> List[Recommendation]:
    """Get all recommendations for a specific database across all procedures"""
    with get_conn_ctx() as conn:
        # Get procedure calls for this database
        cur = conn.execute("SELECT pc_id FROM Procedure_call WHERE db_id = ?", (db_id,))
        pc_ids = [row[0] for row in cur.fetchall()]

        if not pc_ids:
            return []

        # Create placeholders for IN clause
        placeholders = ','.join(['?'] * len(pc_ids))

        # Get all recommendations for this database
        query = f"""
            SELECT r.id_recom, r.description, r.sql_command,
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

        recommendations: List[Recommendation] = []
        for row in cur.fetchall():
            recommendations.append(Recommendation(
                id_recom=row[0],
                description=row[1],
                sql_command=row[2],
                pb_id=row[3],
                pbi_id=row[4],
                pbc_id=row[5],
                created_at=row[6]
            ))

        return recommendations


def get_recommendation(db_id: int, id_recom: int) -> Optional[Recommendation]:
    """Get a specific recommendation by ID for a database"""
    with get_conn_ctx() as conn:
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
                   r.pb_id, r.pbi_id, r.pbc_id, r.created_at,
                   pb.procedure_order as pb_procedure_order,
                   pbi.procedure_order as pbi_procedure_order,
                   pbc.procedure_order as pbc_procedure_order
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
                created_at=row[6],
                pb_procedure_order=row[7],
                pbi_procedure_order=row[8],
                pbc_procedure_order=row[9]
            )
        return None


def get_recommendations_for_record(procedure_name: str, record_id: int) -> List[Recommendation]:
    """Get all recommendations for a specific record"""
    # Map procedure names to foreign key fields
    fk_mapping = {
        "sp_Blitz": "pb_id",
        "sp_BlitzIndex": "pbi_id",
        "sp_BlitzCache": "pbc_id"
    }

    if procedure_name not in fk_mapping:
        raise ValueError(f"Unsupported procedure: {procedure_name}")

    fk_field = fk_mapping[procedure_name]

    with get_conn_ctx() as conn:
        query = f"""
            SELECT id_recom, description, sql_command, pb_id, pbi_id, pbc_id, created_at
            FROM Recommendation
            WHERE {fk_field} = ?
            ORDER BY created_at DESC
        """

        cur = conn.execute(query, (record_id,))
        recommendations: List[Recommendation] = []

        for row in cur.fetchall():
            recommendations.append(Recommendation(
                id_recom=row[0],
                description=row[1],
                sql_command=row[2],
                pb_id=row[3],
                pbi_id=row[4],
                pbc_id=row[5],
                created_at=row[6]
            ))

        return recommendations


def get_db_indexes(pbi_id: int) -> List[DBIndexRecord]:
    """
    Get all DB_Indexes records for a given pbi_id

    Args:
        pbi_id: The BlitzIndex record ID

    Returns:
        List of DBIndexRecord objects
    """
    with get_conn_ctx() as conn:
        cur = conn.execute("""
            SELECT di_id, pbi_id, db_schema_object_indexid, index_definition,
                   secret_columns, fill_factor, index_usage_summary, index_op_stats,
                   index_size_summary, partition_compression_detail, index_lock_wait_summary,
                   is_referenced_by_foreign_key, fks_covered_by_index, last_user_seek,
                   last_user_scan, last_user_lookup, last_user_update, create_date,
                   modify_date, page_latch_wait_count, page_latch_wait_time,
                   page_io_latch_wait_count, page_io_latch_wait_time, create_tsql, drop_tsql
            FROM DB_Indexes
            WHERE pbi_id = ?
            ORDER BY di_id
        """, (pbi_id,))

        records: List[DBIndexRecord] = []
        for row in cur.fetchall():
            row_dict = _row_to_dict(cur, row)
            records.append(DBIndexRecord(**row_dict))

        return records


def get_db_findings(pbi_id: int) -> List[DBFindingRecord]:
    """
    Get all DB_Findings records for a given pbi_id

    Args:
        pbi_id: The BlitzIndex record ID

    Returns:
        List of DBFindingRecord objects
    """
    with get_conn_ctx() as conn:
        cur = conn.execute("""
            SELECT df_id, pbi_id, finding, url, estimated_benefit,
                   missing_index_request, estimated_impact, create_tsql, sample_query_plan
            FROM DB_Findings
            WHERE pbi_id = ?
            ORDER BY df_id
        """, (pbi_id,))

        records: List[DBFindingRecord] = []
        for row in cur.fetchall():
            row_dict = _row_to_dict(cur, row)
            records.append(DBFindingRecord(**row_dict))

        return records


def delete_recommendation(id_recom: int) -> bool:
    """Delete a recommendation by its ID

    Args:
        id_recom: The ID of the recommendation to delete

    Returns:
        bool: True if recommendation was deleted, False if not found

    Raises:
        pyodbc.Error: If database error occurs
    """
    with get_conn_ctx() as conn:
        try:
            cur = conn.execute("DELETE FROM Recommendation WHERE id_recom = ?", (id_recom,))
            deleted_rows = cur.rowcount
            return deleted_rows > 0
        except pyodbc.Error:
            conn.rollback()
            logger.exception("Failed to delete recommendation id=%s", id_recom)
            raise


def extract_exec_parameters(more_info: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract DatabaseName, SchemaName, and TableName from EXEC command in more_info

    Args:
        more_info: String containing EXEC command like "EXEC dbo.sp_BlitzIndex @DatabaseName='AdventureWorks2019', @SchemaName='Production', @TableName='Product';"

    Returns:
        Tuple of (database_name, schema_name, table_name) or (None, None, None) if not found
    """
    if not more_info or not more_info.strip().upper().startswith("EXEC"):
        return None, None, None

    try:
        # Use regex to extract parameter values
        database_match = re.search(r"@DatabaseName\s*=\s*['\"]([^'\"]+)['\"]", more_info, re.IGNORECASE)
        schema_match = re.search(r"@SchemaName\s*=\s*['\"]([^'\"]+)['\"]", more_info, re.IGNORECASE)
        table_match = re.search(r"@TableName\s*=\s*['\"]([^'\"]+)['\"]", more_info, re.IGNORECASE)

        database_name = database_match.group(1) if database_match else None
        schema_name = schema_match.group(1) if schema_match else None
        table_name = table_match.group(1) if table_match else None

        return database_name, schema_name, table_name
    except (AttributeError, TypeError):
        return None, None, None


def update_blitzindex_exec_parameters(pbi_id: int, more_info: str) -> bool:
    """
    Update BlitzIndex record with extracted EXEC parameters

    Args:
        pbi_id: The procedure_blitzindex record ID
        more_info: String containing EXEC command

    Returns:
        True if update was successful, False otherwise
    """
    database_name, schema_name, table_name = extract_exec_parameters(more_info)

    if not any([database_name, schema_name, table_name]):
        return False

    with get_conn_ctx() as conn:
        try:
            conn.execute("""
                UPDATE Procedure_blitzindex
                SET database_name = ?, schema_name = ?, table_name = ?
                WHERE pbi_id = ?
            """, (database_name, schema_name, table_name, pbi_id))

            return True
        except pyodbc.Error:
            conn.rollback()
            logger.exception("Failed to update exec parameters for pbi_id=%s", pbi_id)
            return False


def get_db_indexes_for_record(pbi_id: int) -> List[DBIndexRecord]:
    """Get all DB_Indexes for a specific BlitzIndex record"""
    with get_conn_ctx() as conn:
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
        """, (pbi_id,))

        records: List[DBIndexRecord] = []
        for row in cur.fetchall():
            record_dict = _row_to_dict(cur, row)
            records.append(DBIndexRecord(**record_dict))

        return records


def get_db_findings_for_record(pbi_id: int) -> List[DBFindingRecord]:
    """Get all DB_Findings for a specific BlitzIndex record"""
    with get_conn_ctx() as conn:
        cur = conn.execute("""
            SELECT df_id, pbi_id, finding, url, estimated_benefit,
                   missing_index_request, estimated_impact, create_tsql, sample_query_plan
            FROM DB_Findings
            WHERE pbi_id = ?
        """, (pbi_id,))

        records: List[DBFindingRecord] = []
        for row in cur.fetchall():
            record_dict = _row_to_dict(cur, row)
            records.append(DBFindingRecord(**record_dict))

        return records


def clear_index_findings_for_record(pbi_id: int):
    """Clear all DB_Indexes and DB_Findings for a specific BlitzIndex record"""
    with get_conn_ctx() as conn:
        try:
            conn.execute("DELETE FROM DB_Indexes WHERE pbi_id = ?", (pbi_id,))
            conn.execute("DELETE FROM DB_Findings WHERE pbi_id = ?", (pbi_id,))
            conn.execute("UPDATE Procedure_blitzindex SET index_findings_loaded = FALSE WHERE pbi_id = ?", (pbi_id,))
        except pyodbc.Error:
            conn.rollback()
            logger.exception("Failed to clear index findings for pbi_id=%s", pbi_id)
            raise


def store_db_indexes_for_record(pbi_id: int, indexes: List[Dict[str, Any]]):
    """Store DB_Indexes for a specific BlitzIndex record"""
    with get_conn_ctx() as conn:
        # First, delete existing indexes for this pbi_id
        conn.execute("DELETE FROM DB_Indexes WHERE pbi_id = ?", (pbi_id,))

        for index_data in indexes:
            # Add pbi_id to each record
            index_data['pbi_id'] = pbi_id

            # Create DBIndexRecord for validation
            record = DBIndexRecord(**index_data)
            model_dict = record.model_dump()

            # Build INSERT statement
            fields = []
            values = []
            for field, value in model_dict.items():
                if value is not None:
                    fields.append(field)
                    values.append(value)

            if fields:
                placeholders = ["?"] * len(fields)
                sql = f"INSERT INTO DB_Indexes ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                conn.execute(sql, values)




def store_db_findings_for_record(pbi_id: int, findings: List[Dict[str, Any]]):
    """Store DB_Findings for a specific BlitzIndex record"""
    with get_conn_ctx() as conn:
        # Delete existing findings once
        conn.execute("DELETE FROM DB_Findings WHERE pbi_id = ?", (pbi_id,))

        for finding_data in findings:
            # Add pbi_id to each record
            finding_data['pbi_id'] = pbi_id

            # Create DBFindingRecord for validation
            record = DBFindingRecord(**finding_data)
            model_dict = record.model_dump()

            # Build INSERT statement
            fields = []
            values = []
            for field, value in model_dict.items():
                if value is not None:
                    fields.append(field)
                    values.append(value)

            if fields:
                placeholders = ["?"] * len(fields)
                sql = f"INSERT INTO DB_Findings ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                conn.execute(sql, values)

def mark_index_findings_loaded(pbi_id: int):
    """Mark BlitzIndex record as having index findings loaded"""
    with get_conn_ctx() as conn:
        conn.execute("UPDATE Procedure_blitzindex SET index_findings_loaded = 1 WHERE pbi_id = ?", (pbi_id,))
