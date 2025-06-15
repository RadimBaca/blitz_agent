from flask import Flask, render_template, request, redirect, url_for, jsonify
import pyodbc
import os
import markdown
from markupsafe import Markup
import sqlparse
from dotenv import load_dotenv

from agent_blitz_one_blitzindex import agent_executor
from agent_blitz_one_blitzindex import initial_user_question_template
import result_DAO as dao


load_dotenv()
app = Flask(__name__)

# Seznam podporovaných procedur
PROCEDURES = {
    "Blitz First": "sp_BlitzFirst",
    "Blitz Index": "sp_BlitzIndex",
    "Blitz Cache": "sp_BlitzCache",
}

DISPLAY_KEYS = {
    "Blitz First": ["Finding", "Priority", "URL"],
    "Blitz Index": ["Finding", "Details: schema.table.index(indexid)", "Priority"],
    "Blitz Cache": ["Query Text", "Avg CPU (ms)", "Warnings"],
}

def safe_pretty_json(record: dict) -> dict:
    safe_record = {}
    for k, v in record.items():
        if k == "Query Text":
            safe_record[k] = sqlparse.format(v, keyword_case='upper', output_format='sql', reindent=True)
        else:
            safe_record[k] = v
    return safe_record

def get_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('MSSQL_HOST')};"
        f"DATABASE={os.getenv('MSSQL_DB')};"
        f"UID={os.getenv('MSSQL_USER')};"
        f"PWD={os.getenv('MSSQL_PASSWORD')}"
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
    return redirect(url_for('procedure', proc_name="Blitz First"))

@app.route("/<proc_name>")
def procedure(proc_name):
    records_with_flags = dao.get_all_records(proc_name)
    return render_template("index.html",
                           proc_name=proc_name,
                           procedures=PROCEDURES,
                           records=records_with_flags,
                           display_keys=DISPLAY_KEYS.get(proc_name, []))

@app.route("/init/<proc_name>", methods=["POST"])
def init(proc_name):
    procedure = PROCEDURES[proc_name]
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
            records.append({
                '_json_pretty': safe_pretty_json(full),
                '_full': full
            })

    dao.store_records(proc_name, records)
    dao.delete_chat_sessions(proc_name)
    return redirect(url_for('procedure', proc_name=proc_name))

@app.route("/analyze/<proc_name>/<int:rec_id>", methods=["GET", "POST"])
def analyze(proc_name, rec_id):
    record = dao.get_record(proc_name, rec_id)

    if request.method == "POST":
        user_input = request.form["user_input"]
        chat_history = dao.get_chat_history(proc_name, rec_id) or []
        chat_history.append(("user", user_input))

        result = agent_executor.invoke({
            "input": user_input,
            "chat_history": chat_history
        })
        chat_history.append(("ai", result["output"]))
        dao.store_chat_history(proc_name, rec_id, chat_history)
        return redirect(url_for("analyze", proc_name=proc_name, rec_id=rec_id))

    chat_history = dao.get_chat_history(proc_name, rec_id)
    if not chat_history:
        user_question = initial_user_question_template.format(
            PROCEDURES[proc_name], record["_full"], os.getenv("MSSQL_DB", "sqlbench")
        )
        result = agent_executor.invoke({"input": user_question, "chat_history": []})
        chat_history = [("user", user_question), ("ai", result["output"])]
        dao.store_chat_history(proc_name, rec_id, chat_history)

    return render_template("analyze.html",
                           proc_name=proc_name,
                           rec_id=rec_id,
                           chat_history=chat_history)

if __name__ == "__main__":
    app.run(debug=True)



