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
import src.db_DAO as db_dao

# Global variable to store the current database connection ID
actual_db_id = -1
connections = db_dao.get_all_db_connections()

load_dotenv()
app = Flask(__name__)

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

def get_connection():
    global actual_db_id
    
    try:
        # Get environment variables
        mssql_host = os.getenv('MSSQL_HOST')
        mssql_port = int(os.getenv('MSSQL_PORT', 1433))
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
            new_db_connection = db_dao.DatabaseConnection(
                db_name=mssql_db,
                db_user=mssql_user,
                db_password=mssql_password,
                db_host=mssql_host,
                db_port=mssql_port
            )
            
            # Insert the new connection and get the db_id
            actual_db_id = db_dao.insert_db(new_db_connection)
            
            # Use environment variables for connection
            conn_str = build_connection_string(
                mssql_host,
                mssql_port,
                mssql_db,
                mssql_user,
                mssql_password
            )
        
        return pyodbc.connect(conn_str)
    
    except Exception as e:
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

@app.template_filter("markdown")
def markdown_filter(text):
    return Markup(markdown.markdown(text))

@app.route("/clear_all", methods=["POST"])
def clear_all_route():
    dao.clear_all(actual_db_id)
    return redirect(url_for("home"))

@app.route("/select_database", methods=["POST"])
def select_database():
    """Handle database selection from the combobox"""
    global actual_db_id
    selected_db_id = request.form.get("db_id")
    if selected_db_id:
        actual_db_id = int(selected_db_id)
    
    # Redirect back to the current procedure
    display_name = request.form.get("current_proc", "Blitz")
    return redirect(url_for('procedure', display_name=display_name))

@app.route("/refresh_connections", methods=["POST"])
def refresh_connections():
    """Handle refresh button click to reload connections"""
    # Simply redirect back to refresh the page with updated connections
    display_name = request.form.get("current_proc", "Blitz")
    return redirect(url_for('procedure', display_name=display_name))

@app.route("/add_database", methods=["POST"])
def add_database():
    """Handle adding a new database connection"""
    global actual_db_id
    try:
        # Get form data
        db_name = request.form.get("db_name")
        db_host = request.form.get("db_host")
        db_port = int(request.form.get("db_port", 1433))
        db_user = request.form.get("db_user")
        db_password = request.form.get("db_password")
        
        # Validate required fields
        if not all([db_name, db_host, db_user, db_password]):
            raise ValueError("All fields are required")
        
        # Create new database connection
        new_connection = db_dao.DatabaseConnection(
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            db_host=db_host,
            db_port=db_port
        )
        
        # Insert and set as active
        actual_db_id = db_dao.insert_db(new_connection)
        
        # Redirect back to the current procedure
        display_name = request.form.get("current_proc", "Blitz")
        return redirect(url_for('procedure', display_name=display_name))
        
    except Exception as e:
        # Handle errors - in a real app, you'd want better error handling
        print(f"Error adding database: {e}")
        display_name = request.form.get("current_proc", "Blitz")
        return redirect(url_for('administration'))

@app.route("/delete_database", methods=["POST"])
def delete_database():
    """Handle deleting a database connection"""
    try:
        db_id_to_delete = int(request.form.get("db_id"))
        
        # First clear all data associated with this database
        dao.clear_all(db_id_to_delete)
        
        # Don't allow deleting if it's the only connection
        all_connections = db_dao.get_all_db_connections()
        if len(all_connections) <= 1:
            print("Cannot delete the last database connection")
            display_name = request.form.get("current_proc", "Blitz")
            return redirect(url_for('procedure', display_name=display_name))
        
        # If deleting the currently active database, switch to another one
        global actual_db_id
        if db_id_to_delete == actual_db_id:
            # Find the first connection that's not the one being deleted
            for conn in all_connections:
                if conn.db_id != db_id_to_delete:
                    actual_db_id = conn.db_id
                    break
        
        # Delete the connection
        db_dao.delete_db(db_id_to_delete)
        
        # Redirect back to the current procedure
        display_name = request.form.get("current_proc", "Blitz")
        return redirect(url_for('procedure', display_name=display_name))
        
    except Exception as e:
        print(f"Error deleting database: {e}")
        display_name = request.form.get("current_proc", "Blitz")
        return redirect(url_for('procedure', display_name=display_name))

@app.route("/administration")
def administration():
    """Database administration page"""
    # Get fresh list of connections for the administration page
    current_connections = db_dao.get_all_db_connections()
    return render_template("administration.html",
                           proc_name="Administration",
                           procedures=PROCEDURES,
                           connections=current_connections,
                           actual_db_id=actual_db_id)

@app.route("/")
def home():
    return redirect(url_for('procedure', display_name="Blitz"))

@app.route("/<display_name>")
def procedure(display_name):
    procedure_name = get_procedure_name(display_name)
    records_with_flags = dao.get_all_records(procedure_name, actual_db_id)
    # Get fresh list of connections for the combobox
    current_connections = db_dao.get_all_db_connections()
    return render_template("index.html",
                           proc_name=display_name,
                           procedures=PROCEDURES,
                           records=records_with_flags,
                           display_keys=DISPLAY_KEYS.get(display_name, []),
                           connections=current_connections,
                           actual_db_id=actual_db_id)

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

        dao.store_records(procedure_name, records, actual_db_id)
        # dao.delete_chat_sessions(get_procedure_name(display_name), actual_db_id)
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
    conn = get_connection()
    conn.close()  
    app.run(debug=True, host="0.0.0.0", port=PORT)


