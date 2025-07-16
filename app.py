from flask import Flask, render_template, request, redirect, url_for, jsonify
import pyodbc
import os
import markdown
from markupsafe import Markup
import sqlparse
from dotenv import load_dotenv
import datetime

import src.agent_blitz_one_blitzindex as blitz_agent
import src.result_DAO as dao


def get_procedure_name(display_name: str) -> str:
    """Convert display name to actual procedure name for database operations"""
    return PROCEDURES.get(display_name, display_name)

load_dotenv()
app = Flask(__name__)

# Seznam podporovaných procedur
PROCEDURES = {
    "Blitz": "sp_Blitz",
    "Blitz Index": "sp_BlitzIndex",
    "Blitz Cache": "sp_BlitzCache",
}

DISPLAY_KEYS = {
    "Blitz": ["Finding", "Details", "Priority"],
    "Blitz Index": ["Finding", "Details: schema.table.index(indexid)", "Priority"],
    "Blitz Cache": ["Query Text", "Avg CPU (ms)", "Warnings"],
}

def safe_pretty_json(record: dict) -> dict:
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

def get_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('MSSQL_HOST')};"
        f"DATABASE={os.getenv('MSSQL_DB')};"
        f"UID={os.getenv('MSSQL_USER')};"
        f"PWD={os.getenv('MSSQL_PASSWORD')};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes"
    )
    return pyodbc.connect(conn_str)

@app.template_filter("markdown")
def markdown_filter(text):
    return Markup(markdown.markdown(text))

@app.route("/clear_all", methods=["POST"])
def clear_all_route():
    dao.clear_all()
    return redirect(url_for("home"))

@app.route("/")
def home():
    return redirect(url_for('procedure', display_name="Blitz"))

@app.route("/<display_name>")
def procedure(display_name):
    procedure_name = get_procedure_name(display_name)
    records_with_flags = dao.get_all_records(procedure_name)
    return render_template("index.html",
                           proc_name=display_name,
                           procedures=PROCEDURES,
                           records=records_with_flags,
                           display_keys=DISPLAY_KEYS.get(display_name, []))

@app.route("/init/<display_name>", methods=["POST"])
def init(display_name):
    procedure = PROCEDURES[display_name]
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"EXEC {procedure}")

        while cursor.description is None:
            if not cursor.nextset():
                return "No result sets returned from procedure.", 500

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        records = []
        for row in rows:
            full = dict(zip(columns, row))
            # Apply serialization to both display and storage versions
            serialized_full = safe_pretty_json(full)
            records.append({
                '_json_pretty': serialized_full,
                '_full': serialized_full
            })

    dao.store_records(procedure, records)  # Pass actual procedure name
    dao.delete_chat_sessions(get_procedure_name(display_name))
    return redirect(url_for('procedure', display_name=display_name))

@app.route("/analyze/<display_name>/<int:rec_id>", methods=["GET", "POST"])
def analyze(display_name, rec_id):
    procedure_name = get_procedure_name(display_name)
    record = dao.get_record(procedure_name, rec_id)

    if request.method == "POST":
        user_input = request.form["user_input"]
        chat_history = dao.get_chat_history(procedure_name, rec_id) or []
        chat_history.append(("user", user_input))

        result = blitz_agent.agent_executor.invoke({
            "input": user_input,
            "chat_history": chat_history
        })
        chat_history.append(("ai", result["output"]))
        dao.store_chat_history(procedure_name, rec_id, chat_history)
        return redirect(url_for("analyze", display_name=display_name, rec_id=rec_id))

    chat_history = dao.get_chat_history(procedure_name, rec_id)
    if not chat_history:
        user_question = blitz_agent.initial_user_question_template.format(
            PROCEDURES[display_name], record["_full"], os.getenv("MSSQL_DB", "sqlbench")
        )
        result = blitz_agent.agent_executor.invoke({"input": user_question, "chat_history": []})
        chat_history = [("user", user_question), ("ai", result["output"])]
        dao.store_chat_history(procedure_name, rec_id, chat_history)

    return render_template("analyze.html",
                           proc_name=display_name,
                           rec_id=rec_id,
                           chat_history=chat_history)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)


