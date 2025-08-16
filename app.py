
import os
import json
from flask import Flask, render_template, request, redirect, url_for
import pyodbc
import markdown
from markupsafe import Markup
from dotenv import load_dotenv
from datetime import datetime as dt_parser

import src.agent_blitz_one_blitzindex as blitz_agent
import src.result_DAO as dao
from src.result_DAO import safe_pretty_json
import src.db_DAO as db_dao
import src.db_connection as db_conn


connections = db_dao.get_all_db_connections()

load_dotenv()
app = Flask(__name__)

def get_procedure_name(display_name: str) -> str:
    """Convert display name to actual procedure name for database operations"""
    return PROCEDURES.get(display_name, display_name)

def get_database_error_message(error: Exception, context: str = "operation", display_name: str = None) -> tuple:
    """
    Generate user-friendly error messages and HTML response for database connection issues.

    Args:
        error: The exception that occurred
        context: Context of where the error occurred (e.g., "initialization", "analysis")
        display_name: The display name for the back link (optional)

    Returns:
        A tuple of (HTML response string, HTTP status code)
    """
    error_str = str(error).lower()

    if "timeout" in error_str:
        error_message = f"Database connection timeout during {context}. The database server may be unavailable or overloaded."
    elif "login" in error_str:
        error_message = f"Database login failed during {context}. Please check your database credentials."
    elif "invalid object name" in error_str:
        error_message = f"Database procedure not found during {context}. Please ensure the required procedures exist."
    elif "network" in error_str or "host" in error_str:
        error_message = f"Network connection failed during {context}. Please check your network connectivity."
    else:
        error_message = f"Database connection failed during {context}. Please check your database connection and try again."

    back_link = ""
    if display_name:
        back_link = f"<p><a href='{url_for('procedure', display_name=display_name)}'>← Back to {display_name}</a></p>"

    html_response = f"""
    <h1>Database Connection Error</h1>
    <p><strong>{error_message}</strong></p>
    <p>Please verify:</p>
    <ul>
        <li>Database server is running</li>
        <li>Network connectivity</li>
        <li>Database credentials are correct</li>
        <li>Required stored procedures exist</li>
    </ul>
    <p><strong>Technical details:</strong> {str(error)}</p>
    {back_link}
    """

    return html_response, 500


# Seznam podporovaných procedur
PROCEDURES = {
    "Blitz": "sp_Blitz",
    "Blitz Index": "sp_BlitzIndex",
    "Blitz Cache": "sp_BlitzCache",
}


# Seznam podporovaných procedur
PROCEDURES = {
    "Blitz": "sp_Blitz",
    "Blitz Index": "sp_BlitzIndex",
    "Blitz Cache": "sp_BlitzCache",
}



@app.template_filter("markdown")
def markdown_filter(text):
    return Markup(markdown.markdown(text))

@app.route("/clear_all", methods=["POST"])
def clear_all_route():
    dao.clear_all(db_conn.get_actual_db_id())
    return redirect(url_for("home"))

@app.route("/select_database", methods=["POST"])
def select_database():
    """Handle database selection from the combobox"""
    selected_db_id = request.form.get("db_id")
    if selected_db_id:
        db_conn.set_actual_db_id(int(selected_db_id))

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
        new_db_id = db_dao.insert_db(new_connection)
        db_conn.set_actual_db_id(new_db_id)

        # Redirect back to the current procedure
        display_name = request.form.get("current_proc", "Blitz")
        return redirect(url_for('procedure', display_name=display_name))

    except (pyodbc.Error, ValueError) as e:
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
        current_db_id = db_conn.get_actual_db_id()
        if db_id_to_delete == current_db_id:
            # Find the first connection that's not the one being deleted
            for connection in all_connections:
                if connection.db_id != db_id_to_delete:
                    db_conn.set_actual_db_id(connection.db_id)
                    break

        # Delete the connection
        db_dao.delete_db(db_id_to_delete)

        # Redirect back to the current procedure
        display_name = request.form.get("current_proc", "Blitz")
        return redirect(url_for('procedure', display_name=display_name))

    except (pyodbc.Error, ValueError) as e:
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
                           actual_db_id=db_conn.get_actual_db_id())

@app.route("/")
def home():
    return redirect(url_for('procedure', display_name="Blitz"))

@app.route("/<display_name>")
def procedure(display_name):
    # Skip favicon.ico requests that get caught by this route
    if display_name == 'favicon.ico':
        return '', 404

    # Determine which template to use based on display_name
    if display_name == "Blitz":
        template_name = "blitz.html"
    elif display_name == "Blitz Index":
        template_name = "blitzindex.html"
    elif display_name == "Blitz Cache":
        template_name = "blitzcache.html"
    else:
        return render_template("notfound.html",
                           proc_name=display_name,
                           procedures=PROCEDURES,
                           actual_db_id=db_conn.get_actual_db_id())

    procedure_name = get_procedure_name(display_name)
    blitz_records = dao.get_all_records(procedure_name, db_conn.get_actual_db_id())
    current_connections = db_dao.get_all_db_connections()

    # Handle priority filtering for Blitz and Blitz Index
    if display_name in ["Blitz", "Blitz Index"]:
        blitz_records = [record for record in blitz_records
                    if record.priority is not None and record.priority >= 0]

        max_priority = request.args.get('max_priority')
        if max_priority:
            try:
                max_priority_int = int(max_priority)
                blitz_records = [record for record in blitz_records
                               if record.priority is not None and record.priority <= max_priority_int]
            except ValueError:
                pass  # Invalid priority value, ignore filter

    # Handle finding group filtering for Blitz and Blitz Index
    finding_groups = []
    selected_finding_groups = []

    if display_name == "Blitz":
        # Extract all unique findings from records with valid priority (>= 0) for filter display
        all_groups = set()
        for record in blitz_records:
            if record.finding:
                all_groups.add(record.finding)

        finding_groups = sorted(list(all_groups))

        # Get selected finding groups from request
        selected_finding_groups = request.args.getlist('finding_groups')

        # Check if this is an explicit "deselect all" action
        deselect_all = request.args.get('deselect_all') == 'true'

        if deselect_all:
            # Explicitly show no results
            selected_finding_groups = []
            blitz_records = []
        elif not selected_finding_groups:
            # If no groups selected and not explicit deselect, show all groups (default behavior)
            selected_finding_groups = finding_groups
        else:
            # Filter records by selected finding groups
            blitz_records = [record for record in blitz_records
                           if record.finding and record.finding in selected_finding_groups]

    elif display_name == "Blitz Index":
        # Extract all unique finding groups from records with valid priority (>= 0) for filter display
        all_groups = set()
        for record in blitz_records:
            if record.finding:
                if ':' in record.finding:
                    group = record.finding.split(':', 1)[0]
                    all_groups.add(group)

        finding_groups = sorted(list(all_groups))

        # Get selected finding groups from request
        selected_finding_groups = request.args.getlist('finding_groups')

        # Check if this is an explicit "deselect all" action
        deselect_all = request.args.get('deselect_all') == 'true'

        if deselect_all:
            # Explicitly show no results
            selected_finding_groups = []
            blitz_records = []
        elif not selected_finding_groups:
            # If no groups selected and not explicit deselect, show all groups (default behavior)
            selected_finding_groups = finding_groups
        else:
            # Filter records by selected finding groups
            filtered_records = []
            for record in blitz_records:
                if record.finding and ':' in record.finding:
                    group = record.finding.split(':', 1)[0]
                    if group in selected_finding_groups:
                        filtered_records.append(record)
            blitz_records = filtered_records

    # Handle BlitzCache filtering
    if display_name == "Blitz Cache":
        # Get filter parameters
        min_avg_cpu = request.args.get('min_avg_cpu')
        min_total_cpu = request.args.get('min_total_cpu')
        min_executions = request.args.get('min_executions')
        min_total_reads = request.args.get('min_total_reads')

        # Apply filters if they are provided and valid
        if min_avg_cpu:
            try:
                min_avg_cpu_float = float(min_avg_cpu)
                blitz_records = [record for record in blitz_records
                               if record.avg_cpu_ms is not None and record.avg_cpu_ms >= min_avg_cpu_float]
            except ValueError:
                pass  # Invalid value, ignore filter

        if min_total_cpu:
            try:
                min_total_cpu_float = float(min_total_cpu)
                blitz_records = [record for record in blitz_records
                               if record.total_cpu_ms is not None and record.total_cpu_ms >= min_total_cpu_float]
            except ValueError:
                pass  # Invalid value, ignore filter

        if min_executions:
            try:
                min_executions_int = int(min_executions)
                blitz_records = [record for record in blitz_records
                               if record.executions is not None and record.executions >= min_executions_int]
            except ValueError:
                pass  # Invalid value, ignore filter

        if min_total_reads:
            try:
                min_total_reads_int = int(min_total_reads)
                blitz_records = [record for record in blitz_records
                               if record.total_reads is not None and record.total_reads >= min_total_reads_int]
            except ValueError:
                pass  # Invalid value, ignore filter

        # Handle time window filtering
        start_hour = request.args.get('start_hour')
        end_hour = request.args.get('end_hour')

        if start_hour and end_hour:
            try:
                start_hour_int = int(start_hour)
                end_hour_int = int(end_hour)

                # Filter records based on hour of last_execution
                filtered_records = []
                for record in blitz_records:
                    if record.last_execution:
                        # Extract hour from last_execution datetime
                        try:
                            if hasattr(record.last_execution, 'hour'):
                                # It's a datetime object
                                execution_hour = record.last_execution.hour
                            else:
                                # It's a string, try to parse it
                                execution_hour = None
                                for fmt in ['%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%m/%d/%Y %H:%M:%S']:
                                    try:
                                        dt = dt_parser.strptime(str(record.last_execution), fmt)
                                        execution_hour = dt.hour
                                        break
                                    except ValueError:
                                        continue

                                if execution_hour is None:
                                    continue

                            # Check if execution hour is within the selected range
                            if start_hour_int <= end_hour_int:
                                # Normal range (e.g., 9-17)
                                if start_hour_int <= execution_hour <= end_hour_int:
                                    filtered_records.append(record)
                            else:
                                # Range crosses midnight (e.g., 22-6)
                                if execution_hour >= start_hour_int or execution_hour <= end_hour_int:
                                    filtered_records.append(record)
                        except (AttributeError, ValueError):
                            continue

                blitz_records = filtered_records
            except ValueError:
                pass  # Invalid hour values, ignore filter

        sort_by = request.args.get('sort_by')
        sort_order = request.args.get('sort_order', 'desc')  # Default to descending

        if sort_by in ['avg_cpu_ms', 'total_cpu_ms', 'executions', 'total_reads']:
            reverse = sort_order == 'desc'
            # Sort records, handling None values by placing them at the end
            blitz_records = sorted(blitz_records,
                                 key=lambda x: getattr(x, sort_by) or 0,
                                 reverse=reverse)



    return render_template(template_name,
                           proc_name=display_name,
                           procedures=PROCEDURES,
                           records=blitz_records,
                           connections=current_connections,
                           actual_db_id=db_conn.get_actual_db_id(),
                           current_sort_by=request.args.get('sort_by'),
                           current_sort_order=request.args.get('sort_order', 'desc'),
                           current_max_priority=request.args.get('max_priority'),
                           finding_groups=finding_groups,
                           selected_finding_groups=selected_finding_groups,
                           # BlitzCache filter values
                           current_min_avg_cpu=request.args.get('min_avg_cpu'),
                           current_min_total_cpu=request.args.get('min_total_cpu'),
                           current_min_executions=request.args.get('min_executions'),
                           current_min_total_reads=request.args.get('min_total_reads'),
                           current_start_hour=request.args.get('start_hour'),
                           current_end_hour=request.args.get('end_hour'))

@app.route("/init/<display_name>", methods=["POST"])
def init(display_name):
    try:
        procedure_name = get_procedure_name(display_name)

        # Get database name for procedures that require it
        db_connection = db_dao.get_db(db_conn.get_actual_db_id())
        database_name = db_connection.db_name if db_connection else None

        with db_conn.get_connection() as db_connection:
            cursor = db_connection.cursor()

            # Add @DatabaseName parameter for sp_BlitzCache and sp_BlitzIndex
            if procedure_name == 'sp_BlitzCache' and database_name:
                cursor.execute(f"EXEC {procedure_name} @DatabaseName = ?", (database_name,))
            elif procedure_name == 'sp_BlitzIndex' and database_name:
                cursor.execute(f"EXEC {procedure_name} @IncludeInactiveIndexes=1, @Mode=4, @DatabaseName = ?", (database_name,))
            else:
                cursor.execute(f"EXEC {procedure_name}")

            while cursor.description is None:
                if not cursor.nextset():
                    return f"<h1>Error</h1><p>No result sets returned from procedure {procedure_name}.</p><p><a href='{url_for('procedure', display_name=display_name)}'>← Back to {display_name}</a></p>", 500

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            records = []
            for row in rows:
                full = dict(zip(columns, row))
                # Apply serialization for storage
                serialized_full = safe_pretty_json(full)
                # Store the original SQL Server data for the DAO
                records.append(serialized_full)

        dao.store_records(procedure_name, records, db_conn.get_actual_db_id())
        # dao.delete_chat_sessions(get_procedure_name(display_name), actual_db_id)
        return redirect(url_for('procedure', display_name=display_name))

    except (pyodbc.Error, ValueError) as e:
        return get_database_error_message(e, "initialization", display_name)

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
            # Special handling for Over-Indexing BlitzIndex records
            print(f"Procedure name: {procedure_name}, record.finding: {record.finding}")
            user_question = None
            if (procedure_name == "sp_BlitzIndex" and
                record.finding and record.finding.startswith("Over-Indexing")):

                try:
                    # Execute the SQL command using the new refactored method
                    dao.process_over_indexing_analysis(record)

                    # Get the stored index data for analysis
                    db_indexes = dao.get_db_indexes(record.pbi_id)

                    # Load the over-indexing specific prompt
                    user_question = blitz_agent._load_over_indexing_prompt(
                        record, db_indexes, db_conn.get_actual_db_name()
                    )
                except (pyodbc.Error, ValueError, KeyError) as e:
                    print(f"Error processing over-indexing analysis: {e}")

            if user_question is None:
                # Standard analysis for other types
                raw_record_data = json.loads(record.raw_record) if record.raw_record else {}
                user_question = blitz_agent._load_prompt_for(
                    procedure_name, raw_record_data, db_conn.get_actual_db_name()
                )

            print(f"Initial user question: {user_question}")
            # For display purposes, show the formatted record data
            record_dict = record.model_dump()
            record_dict.pop("raw_record", None)
            store_user_question = "\n".join(f"**{k}**: {v}" for k, v in record_dict.items())

            result = blitz_agent.agent_executor.invoke({"input": user_question, "chat_history": []})
            chat_history = [("user", store_user_question), ("ai", result["output"])]
            dao.store_chat_history(procedure_name, rec_id, chat_history)

        return render_template("analyze.html",
                            proc_name=display_name,
                            rec_id=rec_id,
                            chat_history=chat_history,
                            procedures=PROCEDURES,
                            connections=db_dao.get_all_db_connections(),
                            actual_db_id=db_conn.get_actual_db_id())
    except (pyodbc.Error, ValueError) as e:
        return get_database_error_message(e, "analysis", display_name)

PORT = int(os.getenv("APP_PORT", '5001'))

if __name__ == "__main__":
    conn = db_conn.get_connection()
    conn.close()
    app.run(debug=True, host="0.0.0.0", port=PORT)
