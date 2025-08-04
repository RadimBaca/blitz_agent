import json
from .connection_DAO import _ensure_db, _get_conn

# Initialize database on module import
_ensure_db()

def store_records(proc_name: str, records: list, db_id: int):
    _ensure_db()
    conn = _get_conn()
    try:
        # Get p_id for proc_name
        cur = conn.execute("SELECT p_id FROM Procedure_type WHERE procedure_name = ?", (proc_name,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Procedure_type with procedure_name '{proc_name}' does not exist.")
        p_id = row[0]

        delete_chat_sessions(proc_name, db_id)
        delete_results(proc_name, db_id)
        conn.execute("DELETE FROM Procedure_call WHERE p_id = ? AND db_id = ?", (p_id, db_id))

        # Insert a new Procedure_call with db_id
        conn.execute("INSERT INTO Procedure_call (run, p_id, db_id) VALUES (datetime('now'), ?, ?)", (p_id, db_id))
        pc_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for i, record in enumerate(records):
            # print(f"Storing record {record}")
            conn.execute(
                "INSERT INTO Procedure_result (result, procedure_order, pc_id) VALUES (?, ?, ?)",
                (json.dumps(record), i, pc_id)
            )
        conn.commit()
    finally:
        conn.close()

def get_all_records(proc_name: str, db_id: int):
    _ensure_db()
    conn = _get_conn()
    try:
        # Validate that db_id exists
        cur = conn.execute("SELECT db_id FROM Database_connection WHERE db_id = ?", (db_id,))
        if not cur.fetchone():
            raise ValueError(f"Database connection with db_id '{db_id}' does not exist.")

        cur = conn.execute(
            """
            SELECT pr.result, 
                pr.procedure_order,
                (
                    SELECT COUNT(*) FROM Chat WHERE pr_id = pr.pr_id
                ) AS chat_count
            FROM Procedure_call pc
            JOIN Procedure_type pt ON pc.p_id = pt.p_id
            JOIN Procedure_result pr ON pr.pc_id = pc.pc_id
            WHERE pt.procedure_name = ? AND pc.db_id = ?
            ORDER BY pc.run DESC, pr.procedure_order ASC
            """, (proc_name, db_id)
        )
        records = []
        for rec in cur.fetchall():
            result_json, order, chat_count = rec
            record = json.loads(result_json)
            record["_rec_id"] = order
            record["_analyzed"] = chat_count > 0
            records.append(record)
        return records
    finally:
        conn.close()

def get_record(proc_name: str, rec_id: int):
    _ensure_db()
    conn = _get_conn()
    try:
        cur = conn.execute(
            """
            SELECT pr.result
            FROM Procedure_call pc
            JOIN Procedure_type pt ON pc.p_id = pt.p_id
            JOIN Procedure_result pr ON pr.pc_id = pc.pc_id
            WHERE pt.procedure_name = ? AND pr.procedure_order = ?
            ORDER BY pc.run DESC
            LIMIT 1
            """, (proc_name, rec_id)
        )
        row = cur.fetchone()
        if not row:
            raise IndexError("No record with this rec_id")
        return json.loads(row[0])
    finally:
        conn.close()

def store_chat_history(proc_name: str, rec_id: int, chat_history):
    _ensure_db()
    conn = _get_conn()
    try:
        # Get latest Procedure_call and pr_id for this proc_name and rec_id
        cur = conn.execute(
            """
            SELECT pr.pr_id
            FROM Procedure_call pc
            JOIN Procedure_type pt ON pc.p_id = pt.p_id
            JOIN Procedure_result pr ON pr.pc_id = pc.pc_id
            WHERE pt.procedure_name = ? AND pr.procedure_order = ?
            ORDER BY pc.run DESC
            LIMIT 1
            """, (proc_name, rec_id)
        )
        row = cur.fetchone()
        if not row:
            raise IndexError("No record with this rec_id")
        pr_id = row[0]
        # Remove previous chat for this pr_id
        delete_chat_session(pr_id)
        # Insert chat history as rows, one per tuple, preserving order
        for i, (role, msg) in enumerate(chat_history):
            conn.execute(
                "INSERT INTO Chat (response, type, chat_order, pr_id) VALUES (?, ?, ?, ?)",
                (msg, role, i, pr_id)
            )
        conn.commit()
    finally:
        conn.close()

def get_chat_history(proc_name: str, rec_id: int):
    _ensure_db()
    conn = _get_conn()
    try:
        cur = conn.execute(
            """
            SELECT c.type, c.response
            FROM Procedure_call pc
            JOIN Procedure_type pt ON pc.p_id = pt.p_id
            JOIN Procedure_result pr ON pr.pc_id = pc.pc_id
            JOIN Chat c ON c.pr_id = pr.pr_id
            WHERE pt.procedure_name = ? AND pr.procedure_order = ?
            ORDER BY pc.run DESC, c.chat_order ASC
            """, (proc_name, rec_id)
        )
        chat = [(role, msg) for role, msg in cur.fetchall()]
        if not chat:
            return None
        return chat
    finally:
        conn.close()

def clear_all(db_id: int):
    _ensure_db()
    conn = _get_conn()
    try:
        # Delete chat sessions for this db_id
        conn.execute("""
            DELETE FROM Chat
            WHERE pr_id IN (
                SELECT pr.pr_id FROM Procedure_call pc
                JOIN Procedure_result pr ON pr.pc_id = pc.pc_id
                WHERE pc.db_id = ?
            )
        """, (db_id,))

        # Delete procedure results for this db_id
        conn.execute("""
            DELETE FROM Procedure_result
            WHERE pc_id IN (
                SELECT pc.pc_id FROM Procedure_call pc
                WHERE pc.db_id = ?
            )
        """, (db_id,))

        # Delete procedure calls for this db_id
        conn.execute("DELETE FROM Procedure_call WHERE db_id = ?", (db_id,))
        conn.commit()
    finally:
        conn.close()

def delete_results(proc_name: str, db_id: int):
    _ensure_db()
    conn = _get_conn()
    try:
        conn.execute("""
            DELETE FROM Procedure_result
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
    _ensure_db()
    conn = _get_conn()
    try:
        conn.execute("""
            DELETE FROM Chat
            WHERE pr_id IN (
                SELECT pr.pr_id FROM Procedure_call pc
                JOIN Procedure_type pt ON pc.p_id = pt.p_id
                JOIN Procedure_result pr ON pr.pc_id = pc.pc_id
                WHERE pt.procedure_name = ? AND pc.db_id = ?
            )
        """, (proc_name, db_id))
        conn.commit()
    finally:
        conn.close()


def delete_chat_session(pr_id: int):
    _ensure_db()
    conn = _get_conn()
    try:
        conn.execute("""
            DELETE FROM Chat
            WHERE pr_id = ?
        """, (pr_id,))
        conn.commit()
    finally:
        conn.close()
