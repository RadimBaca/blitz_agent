import os
import sqlite3
import json

DB_DIR = "db"
DB_PATH = os.path.join(DB_DIR, "results.db")
INIT_SQL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "init_results_db.sql")

def _ensure_db():
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

_ensure_db()

def _get_conn():
    return sqlite3.connect(DB_PATH)

def store_records(proc_name: str, records: list):
    _ensure_db()
    conn = _get_conn()
    try:
        # Get p_id for proc_name
        cur = conn.execute("SELECT p_id FROM Procedure_type WHERE procedure_name = ?", (proc_name,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Procedure_type with procedure_name '{proc_name}' does not exist.")
        p_id = row[0]
        # Remove all previous calls/results for this proc_name
        pc_ids = [row[0] for row in conn.execute("SELECT pc_id FROM Procedure_call WHERE p_id = ?", (p_id,))]
        for pc_id in pc_ids:
            pr_ids = [row[0] for row in conn.execute("SELECT pr_id FROM Procedure_result WHERE pc_id = ?", (pc_id,))]
            for pr_id in pr_ids:
                conn.execute("DELETE FROM Chat WHERE pr_id = ?", (pr_id,))
            conn.execute("DELETE FROM Procedure_result WHERE pc_id = ?", (pc_id,))
            conn.execute("DELETE FROM Procedure_call WHERE pc_id = ?", (pc_id,))
        # Insert a new Procedure_call
        conn.execute("INSERT INTO Procedure_call (run, p_id) VALUES (datetime('now'), ?)", (p_id,))
        pc_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for i, record in enumerate(records):
            conn.execute(
                "INSERT INTO Procedure_result (result, procedure_order, pc_id) VALUES (?, ?, ?)",
                (json.dumps(record), i, pc_id)
            )
        conn.commit()
    finally:
        conn.close()

def get_all_records(proc_name: str):
    _ensure_db()
    conn = _get_conn()
    try:
        cur = conn.execute(
            """
            SELECT pr.pr_id, pr.result, pr.procedure_order 
            FROM Procedure_call pc
            JOIN Procedure_type pt ON pc.p_id = pt.p_id
            JOIN Procedure_result pr ON pr.pc_id = pc.pc_id
            WHERE pt.procedure_name = ?
            ORDER BY pc.run DESC, pr.procedure_order ASC
            """, (proc_name,)
        )
        records = []
        for rec in cur.fetchall():
            pr_id, result_json, order = rec
            record = json.loads(result_json)
            record["_rec_id"] = order
            # _analyzed: True if there is any chat for this pr_id
            chat_cur = conn.execute("SELECT COUNT(*) FROM Chat WHERE pr_id = ?", (pr_id,))
            record["_analyzed"] = chat_cur.fetchone()[0] > 0
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
        conn.execute("DELETE FROM Chat WHERE pr_id = ?", (pr_id,))
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

def clear_all():
    _ensure_db()
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM Chat")
        conn.execute("DELETE FROM Procedure_result")
        conn.execute("DELETE FROM Procedure_call")
        conn.commit()
    finally:
        conn.close()

def delete_chat_sessions(proc_name: str):
    _ensure_db()
    conn = _get_conn()
    try:
        conn.execute("""
            DELETE FROM Chat
            WHERE pr_id IN (
                SELECT pr.pr_id FROM Procedure_call pc
                JOIN Procedure_type pt ON pc.p_id = pt.p_id
                JOIN Procedure_result pr ON pr.pc_id = pc.pc_id
                WHERE pt.procedure_name = ?
            )
        """, (proc_name,))
        conn.commit()
    finally:
        conn.close()
