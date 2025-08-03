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

def get_database_error_message(error: Exception, context: str = "operation") -> str:
    """
    Generate user-friendly error messages for database connection issues.
    
    Args:
        error: The exception that occurred
        context: Context of where the error occurred (e.g., "initialization", "analysis")
    
    Returns:
        A user-friendly error message string
    """
    error_str = str(error).lower()
    
    if "timeout" in error_str:
        return f"Database connection timeout during {context}. The database server may be unavailable or overloaded."
    elif "login" in error_str:
        return f"Database login failed during {context}. Please check your database credentials."
    elif "invalid object name" in error_str:
        return f"Database procedure not found during {context}. Please ensure the required procedures exist."
    elif "network" in error_str or "host" in error_str:
        return f"Network connection failed during {context}. Please check your network connectivity."
    else:
        return f"Database connection failed during {context}. Please check your database connection and try again."

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

FILTERING = {
    "Blitz": ["Finding", "Priority"],
    "Blitz Index": ["Finding", "Priority"],
    "Blitz Cache": ["Avg CPU (ms)"],
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
        f"SERVER={os.getenv('MSSQL_HOST')},{os.getenv('MSSQL_PORT')};"
        f"DATABASE={os.getenv('MSSQL_DB')};"
        f"UID={os.getenv('MSSQL_USER')};"
        f"PWD={os.getenv('MSSQL_PASSWORD')};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
        f"Connection Timeout=60;"
        f"Command Timeout=120;"
        f"Authentication=SqlPassword"
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
    try:
        procedure_name = get_procedure_name(display_name)
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"EXEC {procedure_name}")

            while cursor.description is None:
                if not cursor.nextset():
                    return f"<h1>Error</h1><p>No result sets returned from procedure {procedure_name}.</p><p><a href='{url_for('procedure', display_name=display_name)}'>← Back to {display_name}</a></p>", 500

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

        dao.store_records(procedure_name, records) 
        # dao.delete_chat_sessions(get_procedure_name(display_name))
        return redirect(url_for('procedure', display_name=display_name))
    
    except Exception as e:
        error_message = get_database_error_message(e, "initialization")
        
        return f"""
        <h1>Database Connection Error</h1>
        <p><strong>{error_message}</strong></p>
        <p>Please verify:</p>
        <ul>
            <li>Database server is running</li>
            <li>Network connectivity</li>
            <li>Database credentials are correct</li>
            <li>Required stored procedures exist</li>
        </ul>
        <p><strong>Technical details:</strong> {str(e)}</p>
        <p><a href='{url_for('procedure', display_name=display_name)}'>← Back to {display_name}</a></p>
        """, 500

@app.route("/analyze/<display_name>/<int:rec_id>", methods=["GET", "POST"])
def analyze(display_name, rec_id):
    try:
        procedure_name = get_procedure_name(display_name)
        record = dao.get_record(procedure_name, rec_id)

        if request.method == "POST":
            user_input = request.form["user_input"]
            # TODO - replace with just adding to chat history (no rewriting)
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
                get_procedure_name(display_name), record["_full"], os.getenv("MSSQL_DB", "sqlbench")
            )
            store_user_question = "\n".join(f"**{k}**: {v}" for k, v in record["_full"].items())
            result = blitz_agent.agent_executor.invoke({"input": user_question, "chat_history": []})
            chat_history = [("user", store_user_question), ("ai", result["output"])]
            dao.store_chat_history(procedure_name, rec_id, chat_history)

        return render_template("analyze.html",
                            proc_name=display_name,
                            rec_id=rec_id,
                            chat_history=chat_history)
    except Exception as e:
        error_message = get_database_error_message(e, "initialization")
        
        return f"""
        <h1>Database Connection Error</h1>
        <p><strong>{error_message}</strong></p>
        <p>Please verify:</p>
        <ul>
            <li>Database server is running</li>
            <li>Network connectivity</li>
            <li>Database credentials are correct</li>
            <li>Required stored procedures exist</li>
        </ul>
        <p><strong>Technical details:</strong> {str(e)}</p>
        <p><a href='{url_for('procedure', display_name=display_name)}'>← Back to {display_name}</a></p>
        """, 500

PORT = int(os.getenv("APP_PORT", 5001))

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=PORT)


