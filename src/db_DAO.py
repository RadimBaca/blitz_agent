from typing import Optional
from pydantic import BaseModel, Field
from .connection_DAO import _ensure_db, _get_conn


class DatabaseConnection(BaseModel):
    """Pydantic model for database connection information"""
    db_id: Optional[int] = Field(None, description="Database connection ID")
    db_name: str = Field(..., min_length=1, max_length=100, description="Database name")
    db_user: str = Field(..., min_length=1, max_length=100, description="Database username")
    db_password: str = Field(..., min_length=1, max_length=100, description="Database password")
    db_host: str = Field(..., min_length=1, max_length=100, description="Database host")
    db_port: int = Field(..., ge=1, le=65535, description="Database port")
    # optional server metadata
    version: Optional[str] = Field(None, description="Database server version")
    instance_memory_mb: Optional[int] = Field(None, description="Instance memory in MB")


def get_db(db_id: int) -> Optional[DatabaseConnection]:
    """
    Get database connection info by db_id.

    Args:
        db_id: The database connection ID

    Returns:
        DatabaseConnection object if found, None if not found

    Raises:
        ValueError: If db_id is not a positive integer
    """
    if not isinstance(db_id, int) or db_id <= 0:
        raise ValueError("db_id must be a positive integer")

    _ensure_db()
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT db_id, db_name, db_user, db_password, db_host, db_port, version, instance_memory_mb "
            "FROM Database_connection WHERE db_id = ?",
            (db_id,)
        )
        row = cur.fetchone()
        if not row:
            return None

        return DatabaseConnection(
            db_id=row[0],
            db_name=row[1],
            db_user=row[2],
            db_password=row[3],
            db_host=row[4],
            db_port=row[5],
            version=row[6],
            instance_memory_mb=row[7]
        )
    finally:
        conn.close()


def insert_db(db_connection: DatabaseConnection) -> int:
    """
    Insert a new database connection and return the db_id.

    Args:
        db_connection: DatabaseConnection object (db_id will be ignored)

    Returns:
        The newly created db_id

    Raises:
        ValueError: If db_connection validation fails
        sqlite3.IntegrityError: If database constraints are violated
    """
    # Validate the input using Pydantic
    if not isinstance(db_connection, DatabaseConnection):
        raise ValueError("db_connection must be a DatabaseConnection instance")

    _ensure_db()
    conn = _get_conn()
    try:
        cur = conn.execute(
            "INSERT INTO Database_connection (db_name, db_user, db_password, db_host, db_port, version, instance_memory_mb) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (db_connection.db_name, db_connection.db_user, db_connection.db_password,
             db_connection.db_host, db_connection.db_port, db_connection.version, db_connection.instance_memory_mb)
        )
        db_id = cur.lastrowid
        conn.commit()
        return db_id
    finally:
        conn.close()


def exists_db(host: str, port: int, user_name: str) -> int:
    """
    Check whether a database connection with the given combination of host, port and user_name exists.

    Args:
        host: Database host
        port: Database port
        user_name: Database username

    Returns:
        db_id if a matching connection exists, -1 if not found

    Raises:
        ValueError: If any of the parameters are invalid
    """
    if not host or not isinstance(host, str):
        raise ValueError("host must be a non-empty string")
    if not isinstance(port, int) or port <= 0 or port > 65535:
        raise ValueError("port must be an integer between 1 and 65535")
    if not user_name or not isinstance(user_name, str):
        raise ValueError("user_name must be a non-empty string")

    _ensure_db()
    conn = _get_conn()
    try:
        cur = conn.execute(
            """
            SELECT db_id
            FROM Database_connection
            WHERE db_host = ? AND db_port = ? AND db_user = ?
            LIMIT 1
            """,
            (host, port, user_name)
        )
        row = cur.fetchone()
        return row[0] if row else -1
    finally:
        conn.close()


def delete_db(db_id: int) -> bool:
    """
    Delete a database connection by db_id.

    Args:
        db_id: The database connection ID to delete

    Returns:
        True if a connection was deleted, False if no connection was found

    Raises:
        ValueError: If db_id is not a positive integer
        sqlite3.IntegrityError: If there are foreign key constraints preventing deletion
    """
    if not isinstance(db_id, int) or db_id <= 0:
        raise ValueError("db_id must be a positive integer")

    _ensure_db()
    conn = _get_conn()
    try:
        cur = conn.execute("DELETE FROM Database_connection WHERE db_id = ?", (db_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def get_all_db_connections() -> list[DatabaseConnection]:
    """
    Get all database connections.

    Returns:
        List of DatabaseConnection objects
    """
    _ensure_db()
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT db_id, db_name, db_user, db_password, db_host, db_port, version, instance_memory_mb "
            "FROM Database_connection ORDER BY db_id"
        )
        connections = []
        for row in cur.fetchall():
            connections.append(DatabaseConnection(
                db_id=row[0],
                db_name=row[1],
                db_user=row[2],
                db_password=row[3],
                db_host=row[4],
                db_port=row[5],
                version=row[6],
                instance_memory_mb=row[7]
            ))
        return connections
    finally:
        conn.close()
