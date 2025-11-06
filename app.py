
import os
from flask import Flask, render_template, request, redirect, url_for, flash, session
import pyodbc
import markdown
from markupsafe import Markup
from dotenv import load_dotenv
from datetime import datetime as dt_parser

from sqlalchemy import false

import src.agent_blitz_one_blitzindex as blitz_agent
import src.result_DAO as dao
import src.db_DAO as db_dao
import src.db_connection as db_conn
import src.app_filter_sort as filter_sort


connections = db_dao.get_all_db_connections()

load_dotenv()
app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')

def get_procedure_name(display_name: str) -> str:
    """Convert display name to actual procedure name for database operations"""
    return PROCEDURES.get(display_name, display_name)

def redirect_to_display_name(display_name: str):
    """Handle redirection to the correct route based on display name"""
    if display_name == "Administration":
        return redirect(url_for('administration'))
    elif display_name == "Recommendations":
        return redirect(url_for('recommendations'))
    else:
        return redirect(url_for('procedure', display_name=display_name))

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

def load_indexes_findings(record):
    """Load index findings for a BlitzIndex record if not already loaded."""
    if not record.index_findings_loaded and record.more_info and record.more_info.lower().startswith("exec"):
        print(f"Processing more_info for {record.more_info}")
        try:
            dao.process_more_info(record)
        except (pyodbc.Error, ValueError, KeyError) as e:
            flash(f"Error loading index details: {str(e)}", "error")


def perform_initial_analysis(procedure_name, record, procedure_order):
    """Perform the initial automated analysis for a record using an agent and store chat history.

    Returns the created chat_history list.
    """
    # Special handling: if BlitzIndex and more_info contains an EXEC, try to load index details
    if (procedure_name == "sp_BlitzIndex" and
        record.more_info and record.more_info.lower().startswith("exec")):
        try:
            dao.process_more_info(record)
        except (pyodbc.Error, ValueError, KeyError) as e:
            print(f"Error processing {record.more_info}: {e}")

    user_question = blitz_agent.load_specialized_prompt(
        procedure_name, record, db_conn.get_actual_db_name()
    )

    print(f"Initial user question: {user_question}")
    # For display purposes, show the formatted record data
    record_dict = record.model_dump()
    record_dict.pop("raw_record", None)
    store_user_question = "\n".join(f"**{k}**: {v}" for k, v in record_dict.items())

    # Get the actual record ID (pb_id, pbi_id, or pbc_id) for recommendations
    actual_record_id = getattr(record, PROCEDURES_ID_MAPPING.get(procedure_name, "pb_id"))

    try:
        result = blitz_agent.execute(
            procedure=procedure_name,
            record_id=actual_record_id,
            user_input=user_question,
            chat_history=[]
        )
        chat_history = [("user", store_user_question), ("ai", result["output"])]
        dao.store_chat_history(procedure_name, procedure_order, chat_history)
    except Exception as e:
        # Handle agent execution errors gracefully
        error_message = f"Analysis failed due to a technical issue: {str(e)}. Please try again."
        chat_history = [("user", store_user_question), ("ai", error_message)]
        app.logger.error(f"Agent execution failed: {str(e)}")

    return chat_history

# Seznam podporovaných procedur
PROCEDURES = {
    "Blitz": "sp_Blitz",
    "Blitz Index": "sp_BlitzIndex",
    "Blitz Cache": "sp_BlitzCache",
}

# Mapping procedure names to record ID fields
PROCEDURES_ID_MAPPING = {
    "sp_Blitz": "pb_id",
    "sp_BlitzIndex": "pbi_id",
    "sp_BlitzCache": "pbc_id"
}



@app.template_filter("markdown")
def markdown_filter(text):
    # Use markdown with configuration to preserve list structure
    md = markdown.Markdown(
        extensions=['fenced_code', 'tables'],
        tab_length=2,
        output_format='html'
    )
    return Markup(md.convert(text))

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
    return redirect_to_display_name(display_name)

@app.route("/refresh_connections", methods=["POST"])
def refresh_connections():
    """Handle refresh button click to reload connections"""
    # Simply redirect back to refresh the page with updated connections
    display_name = request.form.get("current_proc", "Blitz")
    return redirect_to_display_name(display_name)

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

        version, instance_memory_mb = db_conn.probe_db_info(db_host, db_port, db_name, db_user, db_password)
        has_blitz = db_conn.check_blitz_procedures(db_host, db_port, db_name, db_user, db_password)

        # Create new database connection
        new_connection = db_dao.DatabaseConnection(
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            db_host=db_host,
            db_port=db_port,
            version=version,
            instance_memory_mb=instance_memory_mb,
            has_blitz_procedures=has_blitz
        )

        # Insert and set as active
        new_db_id = db_dao.insert_db(new_connection)
        db_conn.set_actual_db_id(new_db_id)

        # Redirect back to the current procedure
        display_name = request.form.get("current_proc", "Blitz")
        return redirect_to_display_name(display_name)

    except (pyodbc.Error, ValueError) as e:
        # Handle errors - in a real app, you'd want better error handling
        print(f"Error adding database: {e}")
        display_name = request.form.get("current_proc", "Blitz")
        return redirect(url_for('administration'))


@app.route("/test_database", methods=["POST"])
def test_database():
    """Handle testing a database connection from the administration form and flash the result."""
    try:
        db_name = request.form.get("db_name")
        db_host = request.form.get("db_host")
        db_port = int(request.form.get("db_port", 1433))
        db_user = request.form.get("db_user")
        db_password = request.form.get("db_password")

        # Store submitted values so administration can prefill the form after redirect
        session['test_form'] = {
            'db_name': db_name,
            'db_host': db_host,
            'db_port': db_port,
            'db_user': db_user,
            # Don't store passwords in session in production; kept here for UX convenience in dev
            'db_password': db_password,
        }

        if not all([db_name, db_host, db_user, db_password]):
            flash("All fields are required to test the connection.", "error")
            return redirect(url_for('administration'))

        version, instance_memory_mb = db_conn.probe_db_info(db_host, db_port, db_name, db_user, db_password)

        # Check for Blitz procedures existence
        has_blitz = db_conn.check_blitz_procedures(db_host, db_port, db_name, db_user, db_password)

        if version or instance_memory_mb or has_blitz is not None:
            parts = []
            if version:
                parts.append(f"Version: {version}")
            if instance_memory_mb is not None:
                parts.append(f"Instance memory: {instance_memory_mb} MB")
            if has_blitz is not None:
                if has_blitz:
                    parts.append("Blitz procedures: Available")
                else:
                    parts.append("Blitz procedures: Not found")
            flash(f"Connection successful. {'; '.join(parts)}", "success")
        else:
            flash("Connection failed. Could not reach the database or insufficient permissions to probe metadata.", "error")

        return redirect(url_for('administration'))

    except (pyodbc.Error, ValueError) as e:
        flash(f"Connection test failed: {str(e)}", "error")
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
            return redirect_to_display_name(display_name)

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
        return redirect_to_display_name(display_name)

    except (pyodbc.Error, ValueError) as e:
        print(f"Error deleting database: {e}")
        display_name = request.form.get("current_proc", "Blitz")
        return redirect_to_display_name(display_name)

@app.route("/install_blitz_procedures", methods=["POST"])
def install_blitz_procedures():
    """Handle installing Blitz procedures for a database connection"""
    try:
        db_id = int(request.form.get("db_id"))

        # Get the database connection details
        db_connection = db_dao.get_db(db_id)
        if not db_connection:
            flash("Database connection not found.", "error")
            return redirect(url_for('administration'))

        # Install the Blitz procedures
        success, message = db_conn.install_blitz_procedures(
            db_connection.db_host,
            db_connection.db_port,
            db_connection.db_name,
            db_connection.db_user,
            db_connection.db_password
        )

        if success:
            # Update the has_blitz_procedures status
            if db_conn.update_blitz_procedures_status(db_id):
                flash(f"Success! {message}", "success")
            else:
                flash(f"Procedures installed but failed to update status: {message}", "warning")
        else:
            flash(f"Installation failed: {message}", "error")

        return redirect(url_for('administration'))

    except (ValueError, Exception) as e:
        flash(f"Error installing Blitz procedures: {str(e)}", "error")
        return redirect(url_for('administration'))

@app.route("/administration")
def administration():
    """Database administration page"""
    # Get fresh list of connections for the administration page
    current_connections = db_dao.get_all_db_connections()
    # If a test submitted form exists in session, pop it to prefill the form
    test_form = session.pop('test_form', None)
    return render_template("administration.html",
                           proc_name="Administration",
                           procedures=PROCEDURES,
                           connections=current_connections,
                           actual_db_id=db_conn.get_actual_db_id(),
                           test_form=test_form)

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

    if not db_conn.get_actual_db().has_blitz_procedures:
        flash(f"The selected database does not have Blitz procedures installed. Please install them using Install Blitz button.", "error" )
        return redirect(url_for('administration'))

    procedure_name = get_procedure_name(display_name)
    blitz_records = dao.get_all_records(procedure_name, db_conn.get_actual_db_id())
    current_connections = db_dao.get_all_db_connections()

    # Handle priority filtering for Blitz and Blitz Index
    if display_name in ["Blitz", "Blitz Index"]:
        max_priority = request.args.get('max_priority')
        blitz_records = filter_sort.filter_priority(blitz_records, max_priority)

    # Handle finding group filtering for Blitz and Blitz Index
    finding_groups = []
    selected_finding_groups = []

    if display_name == "Blitz":
        # Extract all unique findings from records
        blitz_records = filter_sort.filter_blitz(blitz_records)

    elif display_name == "Blitz Index":
        # Extract all unique finding groups from records
        blitz_records, finding_groups, selected_finding_groups = filter_sort.filter_blitz_index(blitz_records)

    # Handle BlitzCache filtering
    if display_name == "Blitz Cache":
        # Get filter parameters
        min_avg_cpu = request.args.get('min_avg_cpu')
        min_total_cpu = request.args.get('min_total_cpu')
        min_executions = request.args.get('min_executions')
        min_total_reads = request.args.get('min_total_reads')
        blitz_records = filter_sort.filter_blitz_cache(blitz_records, min_avg_cpu, min_total_cpu, min_executions, min_total_reads)

        # Handle time window filtering
        start_hour = request.args.get('start_hour')
        end_hour = request.args.get('end_hour')
        blitz_records = filter_sort.filter_by_hour(start_hour, end_hour, blitz_records)

        sort_by = request.args.get('sort_by')
        sort_order = request.args.get('sort_order', 'desc')  # Default sort order is descending
        blitz_records = filter_sort.sort_records(blitz_records, sort_by, sort_order)

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

        ok, records = db_conn.exec_blitz(procedure_name)

        if not ok:
            return f"<h1>Error</h1><p>No result sets returned from procedure {procedure_name}.</p><p><a href='{url_for('procedure', display_name=display_name)}'>← Back to {display_name}</a></p>", 500

        dao.store_records(procedure_name, records, db_conn.get_actual_db_id())
        # dao.delete_chat_sessions(get_procedure_name(display_name), actual_db_id)
        return redirect(url_for('procedure', display_name=display_name))

    except (pyodbc.Error, ValueError) as e:
        return get_database_error_message(e, "initialization", display_name)


@app.route("/analyze/<display_name>/<int:procedure_order>", methods=["GET", "POST"])
def analyze(display_name, procedure_order):
    try:
        procedure_name = get_procedure_name(display_name)
        record = dao.get_record(procedure_name, procedure_order, db_conn.get_actual_db_id())

        if request.method == "POST":
            user_input = request.form["user_input"]
            # TODO - replace with just adding to chat history (no rewriting)
            chat_history = dao.get_chat_history(procedure_name, procedure_order) or []
            chat_history.append(("user", user_input))

            # Get the actual record ID (pb_id, pbi_id, or pbc_id) for recommendations
            actual_record_id = getattr(record, PROCEDURES_ID_MAPPING.get(procedure_name, "pb_id"))

            result = blitz_agent.execute(
                procedure=procedure_name,
                record_id=actual_record_id,
                user_input=user_input,
                chat_history=chat_history
            )
            chat_history.append(("ai", result["output"]))
            dao.store_chat_history(procedure_name, procedure_order, chat_history)
            return redirect(url_for("analyze", display_name=display_name, procedure_order=procedure_order))

        chat_history = dao.get_chat_history(procedure_name, procedure_order)
        if not chat_history:

            # Perform initial automated analysis and store chat history
            chat_history = perform_initial_analysis(procedure_name, record, procedure_order)

        return render_template("analyze.html",
                            proc_name=display_name,
                            rec_id=procedure_order,
                            chat_history=chat_history,
                            procedures=PROCEDURES,
                            connections=db_dao.get_all_db_connections(),
                            actual_db_id=db_conn.get_actual_db_id(),
                            record_recommendations=dao.get_recommendations_for_record(
                                procedure_name, getattr(record, PROCEDURES_ID_MAPPING.get(procedure_name, "pb_id"))
                            ))
    except (pyodbc.Error, ValueError) as e:
        return get_database_error_message(e, "analysis", display_name)

@app.route("/analyze_multiple/<display_name>", methods=["POST"])
def analyze_multiple(display_name):
    """Analyze multiple checked Blitz results at once"""
    try:
        procedure_name = get_procedure_name(display_name)

        # Get selected record IDs from the form
        selected_ids = request.form.getlist('selected_records')
        if not selected_ids:
            flash("No records selected for analysis.", "error")
            return redirect(url_for('procedure', display_name=display_name))

        # Convert to integers and validate
        try:
            selected_rec_ids = [int(rec_id) for rec_id in selected_ids]
        except ValueError:
            flash("Invalid record IDs selected.", "error")
            return redirect(url_for('procedure', display_name=display_name))

        # Track analysis statistics
        analysis_stats = {
            'total_records': len(selected_rec_ids),
            'processed_records': 0,
            'recommendations_generated': 0,
            'errors': 0,
            'start_time': dt_parser.now()
        }

        error_details = []

        # Process each selected record
        for rec_id in selected_rec_ids:
            try:
                record = dao.get_record(procedure_name, rec_id, db_conn.get_actual_db_id())
                perform_initial_analysis(procedure_name, record, record.procedure_order)

                actual_record_id = getattr(record, PROCEDURES_ID_MAPPING.get(procedure_name, "pb_id"))
                analysis_stats['processed_records'] += 1

                # Count recommendations generated for this record
                new_recs = dao.get_recommendations_for_record(procedure_name, actual_record_id)
                analysis_stats['recommendations_generated'] += len(new_recs)

            except (pyodbc.Error, ValueError) as e:
                error_details.append(f"Record {rec_id}: {str(e)}")
                analysis_stats['errors'] += 1
                app.logger.error(f"Multi-analysis failed for record {rec_id}: {str(e)}")

        analysis_stats['end_time'] = dt_parser.now()
        analysis_stats['processing_time'] = analysis_stats['end_time'] - analysis_stats['start_time']

        # Create flash messages with statistics
        if analysis_stats['processed_records'] > 0:
            flash(f"Multi-analysis completed! Processed {analysis_stats['processed_records']} records, "
                  f"generated {analysis_stats['recommendations_generated']} recommendations in "
                  f"{analysis_stats['processing_time'].total_seconds():.1f} seconds.", "success")

        if analysis_stats['errors'] > 0:
            flash(f"{analysis_stats['errors']} errors occurred during analysis. "
                  f"Details: {'; '.join(error_details[:3])}", "error")

        return redirect(url_for('procedure', display_name=display_name))

    except (pyodbc.Error, ValueError) as e:
        return get_database_error_message(e, "multi-analysis", display_name)

@app.route("/recommendations")
def recommendations():
    """List all recommendations for the current database"""
    try:
        current_connections = db_dao.get_all_db_connections()
        all_recommendations = dao.get_all_recommendations(db_conn.get_actual_db_id())

        return render_template("recommendations.html",
                             proc_name="Recommendations",
                             procedures=PROCEDURES,
                             recommendations=all_recommendations,
                             connections=current_connections,
                             actual_db_id=db_conn.get_actual_db_id())
    except (pyodbc.Error, ValueError) as e:
        return get_database_error_message(e, "loading recommendations")

@app.route("/recommendation/<int:id_recom>")
def recommendation_detail(id_recom):
    """Show detail of a specific recommendation"""
    try:
        current_connections = db_dao.get_all_db_connections()
        recommendation = dao.get_recommendation(db_conn.get_actual_db_id(), id_recom)

        if not recommendation:
            return render_template("notfound.html",
                                 proc_name="Recommendation Not Found",
                                 procedures=PROCEDURES,
                                 actual_db_id=db_conn.get_actual_db_id())

        return render_template("recommendation_detail.html",
                             proc_name=f"Recommendation #{id_recom}",
                             procedures=PROCEDURES,
                             recommendation=recommendation,
                             connections=current_connections,
                             actual_db_id=db_conn.get_actual_db_id())
    except (pyodbc.Error, ValueError) as e:
        return get_database_error_message(e, "loading recommendation")

@app.route("/recommendation/<int:id_recom>/delete", methods=['POST'])
def delete_recommendation(id_recom):
    """Delete a specific recommendation"""
    try:
        # First check if recommendation exists
        recommendation = dao.get_recommendation(db_conn.get_actual_db_id(), id_recom)

        if not recommendation:
            flash("Recommendation not found.", "error")
            return redirect(url_for('recommendations'))

        # Delete the recommendation
        deleted = dao.delete_recommendation(id_recom)

        if deleted:
            flash(f"Recommendation #{id_recom} has been successfully deleted.", "success")
        else:
            flash("Failed to delete recommendation.", "error")

        return redirect(url_for('recommendations'))

    except (pyodbc.Error, ValueError) as e:
        return get_database_error_message(e, "deleting recommendation")


@app.route("/index_details/<int:procedure_order>", methods=["GET", "POST"])
def index_details(procedure_order):
    """Show detailed index and finding information for a BlitzIndex record"""
    try:
        # Get the BlitzIndex record
        blitzindex_record = dao.get_record("sp_BlitzIndex", procedure_order, db_conn.get_actual_db_id())
        if not blitzindex_record:
            return "Problem loading Blitz record", 404

        load_indexes_findings(blitzindex_record)

        # Handle reload request
        if request.method == "POST" and request.form.get("action") == "reload":
            # Clear existing data
            dao.clear_index_findings_for_record(blitzindex_record.pbi_id)
            load_indexes_findings(blitzindex_record)

        # Get indexes and findings
        db_indexes = dao.get_db_indexes_for_record(blitzindex_record.pbi_id)
        db_findings = dao.get_db_findings_for_record(blitzindex_record.pbi_id)

        print(f"Found {len(db_indexes)} indexes and {len(db_findings)} findings for pbi_id={procedure_order}")

        return render_template("index_details.html",
                             proc_name="Blitz Index",
                             procedures=PROCEDURES,
                             connections=db_dao.get_all_db_connections(),
                             actual_db_id=db_conn.get_actual_db_id(),
                             blitzindex_record=blitzindex_record,
                             db_indexes=db_indexes,
                             db_findings=db_findings)

    except (pyodbc.Error, ValueError) as e:
        return get_database_error_message(e, "loading index details", "Blitz Index")


PORT = int(os.getenv("APP_PORT", '5001'))

if __name__ == "__main__":
    conn = db_conn.get_connection()
    conn.close()
    app.run(debug=True, host="0.0.0.0", port=PORT)
