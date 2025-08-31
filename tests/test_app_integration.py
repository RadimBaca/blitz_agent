import os
import pytest
import requests
from pathlib import Path
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

import src.db_DAO as db_dao
from src.db_DAO import DatabaseConnection
from src.connection_DAO import _get_conn
import src.db_connection as db_conn
import src.result_DAO as dao
from app import app as flask_app

sys.path.append(os.path.join(os.path.dirname(__file__), 'adventureworks_workload'))
try:
    import init_adventure_works as init_aw
    import workload_generator as wg_main
except ImportError:
    # Handle case when modules are not available
    init_aw = None
    wg_main = None


@pytest.fixture
def server_health_check():
    """Check if server is running on expected port"""
    app_url = os.getenv('APP_URL', 'http://localhost:5001')

    try:
        response = requests.get(f"{app_url}/", timeout=5)
        if response.status_code == 200 or response.status_code == 302:  # 302 for redirect
            return app_url
    except requests.ConnectionError:
        pass

    pytest.skip(f"Server not running on {app_url}. Please start the server first.")
    return False


def server_health_check_with_url(app_url: str):
    """Check if server is running on specified URL

    Args:
        app_url: URL of the application to check

    Returns:
        str: The app_url if server is running

    Raises:
        pytest.skip: If server is not running
    """
    try:
        response = requests.get(f"{app_url}/", timeout=5)
        if response.status_code == 200 or response.status_code == 302:  # 302 for redirect
            return app_url
    except requests.ConnectionError:
        pass

    pytest.skip(f"Server not running on {app_url}. Please start the server first.")
    return False


def check_state_db(sql_query: str, text_file_path: str) -> bool:
    """
    Check state database content against expected values from text file

    Args:
        sql_query: SQL query to execute on state database
        text_file_path: Path to text file with expected content

    Returns:
        True if content matches, False otherwise
    """
    # Execute SQL query on state database
    conn = _get_conn()
    cursor = conn.execute(sql_query)
    actual_results = cursor.fetchall()

    # Read expected content from text file
    with open(text_file_path, 'r', encoding='utf-8') as f:
        expected_content = f.read().strip()

    # Convert actual results to string for comparison
    actual_content = '\n'.join([str(row) for row in actual_results])

    print(f"Actual content:\n{actual_content}")
    print(f"Expected content:\n{expected_content}")

    return actual_content.strip() == expected_content


class TestAppIntegration:
    """Integration tests for app.py with Adventure Works database"""

    @pytest.mark.slow
    def test_adventure_works_integration(self, server_health_check):
        """
        Complete integration test following specified steps:
        1. Run Adventure Works initialization and workload generator
        2. Delete results.db file
        3. Call init endpoint to create Database_connection record
        4. Check Database_connection content in state database
        5. Call procedure endpoint with Blitz Index parameter
        6. Check that state database contains only one record with priority 10
        """

        # Get the server URL from the fixture
        app_url = server_health_check

        # Load environment from Adventure Works directory
        test_env_path = Path(__file__).parent / 'adventureworks_workload' / '.env'
        assert test_env_path.exists(), f"Adventure Works .env file not found at {test_env_path}"
        load_dotenv(test_env_path)

        assert server_health_check

        # Step 1: Run Adventure Works initialization and workload generator
        print("Step 1: Running Adventure Works initialization...")


        # Verify environment variables are loaded
        assert os.getenv('MOCK_MSSQL_DB'), "MOCK_MSSQL_DB environment variable not found"
        assert os.getenv('MOCK_MSSQL_HOST'), "MOCK_MSSQL_HOST environment variable not found"
        print("✓ Environment variables loaded successfully")

        # Run initialization (this would normally create indexes)
        try:
            # Note: This might fail if the database is not accessible,
            # but we'll continue with the test for the state database parts
            if init_aw:
                # Call the create_over_indexing_scenario function directly
                init_aw.create_over_indexing_scenario()
                print("✓ Adventure Works over-indexing initialization completed")

                # Call the create_heap_table_scenario function
                init_aw.create_heap_table_scenario()
                print("✓ Adventure Works heap table conversion completed")
            else:
                print("⚠ Adventure Works module not available - skipping initialization")
        except (ConnectionError, ImportError, AttributeError) as e:
            print(f"Adventure Works initialization failed (expected in test environment): {e}")

        # Run workload generator
        try:
            if wg_main:
                wg_main.execute_workload(duration_minutes=0.1)  # Reduced from 0.5 to 0.1 minutes
                print("✓ Workload generator completed")
            else:
                print("⚠ Workload generator module not available - skipping")
        except (ConnectionError, ImportError, AttributeError) as e:
            print(f"Workload generator failed (expected in test environment): {e}")

        # Step 2: Delete results.db file if it exists
        print("Step 2: Deleting results.db file...")
        results_db_path = Path("db") / "results.db"
        if results_db_path.exists():
            results_db_path.unlink()
            print("✓ results.db file deleted")
        else:
            print("✓ results.db file did not exist")

        # Step 3: Call init endpoint
        print("Step 3: Calling init endpoint...")

        # First, create a database connection by POSTing to the app's /add_database
        # endpoint so the application path is exercised end-to-end.
        db_name = os.getenv('MOCK_MSSQL_DB', 'AdventureWorks2019')
        db_user = os.getenv('MOCK_MSSQL_USER', 'k2_zoot')
        db_password = os.getenv('MOCK_MSSQL_PASSWORD', 'cLaS8eJoA5')
        db_host = os.getenv('MOCK_MSSQL_HOST', 'bayer.cs.vsb.cz')
        db_port = int(os.getenv('MOCK_MSSQL_PORT', '50042'))

        # Use the Flask test client to call the add_database endpoint
        with flask_app.test_client() as client:
            resp = client.post(
                "/add_database",
                data={
                    "db_name": db_name,
                    "db_host": db_host,
                    "db_port": str(db_port),
                    "db_user": db_user,
                    "db_password": db_password,
                    "current_proc": "Blitz",
                },
                follow_redirects=False,
            )

        # Expect a redirect (302) or OK (200)
        assert resp.status_code in (200, 302), f"Unexpected response creating DB via /add_database: {resp.status_code}"

        # Verify the connection was created in the state DB and set active.
        connections = db_dao.get_all_db_connections()
        matching = [
            c for c in connections
            if c.db_name == db_name and c.db_host == db_host and c.db_user == db_user
        ]
        assert matching, "New database connection not found after POST /add_database"
        new_db = matching[-1]
        new_db_id = new_db.db_id
        assert new_db_id is not None and new_db_id > 0, "Failed to create database connection"

        # Ensure the app active DB is set to the new connection (add_database sets it)
        db_conn.set_actual_db_id(new_db_id)
        db_conn.set_actual_db_id(new_db_id)

        print(f"✓ Database connection created with ID: {new_db_id}")

        # Step 4: Check Database_connection content in state database
        print("Step 4: Checking Database_connection in state database...")

        # Query to check Database_connection table
        db_conn_query = """
        SELECT db_id, db_name, db_user, db_host, db_port
        FROM Database_connection
        ORDER BY db_id
        """

        # Check Database_connection content
        db_conn_check = check_state_db(db_conn_query, "tests/expected_database_connection.txt")
        assert db_conn_check, "Database_connection content does not match expected values"
        print("✓ Database_connection content verified")

        # Step 5: Call procedure endpoint with Blitz Index parameter
        print("Step 5: Calling procedure endpoint with Blitz Index...")

        # Make HTTP request to the running server
        response = requests.post(f"{app_url}/init/Blitz Index", timeout=120)  # Increased timeout for potential LLM operations
        print(f"Init endpoint response status: {response.status_code}")

        # Assert that the endpoint call was successful
        assert response.status_code in [200, 302], f"Init endpoint failed with status {response.status_code}. Response: {response.text[:500]}"

        if response.status_code == 200:
            print("✓ Init endpoint called successfully")
        elif response.status_code == 302:
            print("✓ Init endpoint returned redirect (likely successful)")

        # Step 6: Check state database for priority 10 record
        print("Step 6: Checking for priority 10 record in state database...")

        # Query to check for records with priority 10 using check_state_db procedure
        priority_query = """
        SELECT COUNT(*)
        FROM Procedure_blitzindex
        WHERE priority = 10
        """

        # Use check_state_db procedure to verify the priority 10 record
        expected_priority_file = "tests/expected_priority_10.txt"
        priority_check_result = check_state_db(priority_query, expected_priority_file)
        assert priority_check_result, f"Priority 10 record check failed. Expected content in {expected_priority_file} does not match actual query result."
        print("✓ Priority 10 record found in state database")

        # Step 7: Get the rec_id (procedure_order) of the priority 10 record for analyze endpoint
        print("Step 7: Getting rec_id of priority 10 record...")

        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute("""
        SELECT procedure_order, finding
        FROM Procedure_blitzindex
        WHERE priority = 10
        LIMIT 1
        """)
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        assert result is not None, "No priority 10 record found to get procedure_order"
        procedure_order = result[0]
        finding = result[1]
        print(f"✓ Found priority 10 record with procedure_order: {procedure_order}, finding: {finding}")

        # Verify it's an over-indexing record
        assert finding and finding.startswith("Over-Indexing"), f"Expected Over-Indexing finding, got: {finding}"
        print("✓ Confirmed record is Over-Indexing type")

        # Step 8: Call analyze endpoint for the priority 10 record
        print("Step 8: Calling analyze endpoint for over-indexing analysis...")

        analyze_url = f"{app_url}/analyze/Blitz Index/{procedure_order}"
        response = requests.get(analyze_url, timeout=300)  # 5 minutes for LLM processing
        print(f"Analyze endpoint response status: {response.status_code}")

        # Assert that the analyze endpoint call was successful
        assert response.status_code in [200, 302], f"Analyze endpoint failed with status {response.status_code}. Response: {response.text[:500]}"
        print("✓ Analyze endpoint called successfully")

        # Step 9: Verify that 12 records are inserted into DB_Indexes table
        print("Step 9: Checking DB_Indexes table for Product table indexes...")

        conn = _get_conn()
        cursor = conn.cursor()

        # First, get the pbi_id for our priority 10 record
        cursor.execute("""
        SELECT pbi_id
        FROM Procedure_blitzindex
        WHERE priority = 10 AND procedure_order = ?
        """, (procedure_order,))
        pbi_result = cursor.fetchone()
        assert pbi_result is not None, f"Could not find pbi_id for rec_id {procedure_order}"
        pbi_id = pbi_result[0]

        # Now check how many DB_Indexes records were created for this pbi_id
        cursor.execute("""
        SELECT COUNT(*)
        FROM DB_Indexes
        WHERE pbi_id = ?
        """, (pbi_id,))
        db_indexes_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        print(f"Found {db_indexes_count} records in DB_Indexes table for pbi_id {pbi_id}")

        # Assert that exactly 12 records were inserted (Product table has 12 indexes)
        assert db_indexes_count == 12, f"Expected 12 DB_Indexes records for Product table, but found {db_indexes_count}"
        print("✓ Confirmed 12 DB_Indexes records created for Product table over-indexing analysis")


        # Step 10: Call index_details end point
        print("Step 10: Calling index_details endpoint...")
        index_details_url = f"{app_url}/index_details/{procedure_order}"
        response = requests.get(index_details_url, timeout=10)  # 10 seconds timeout for database response
        print(f"Index details endpoint response status: {response.status_code}")

        # Assert that the index details endpoint call was successful
        assert response.status_code in [200, 302], f"Index details endpoint failed with status {response.status_code}. Response: {response.text[:500]}"
        print("✓ Index details endpoint called successfully")

        # Check that the response contains <table class="indexes-table">
        assert '<table class="indexes-table">' in response.text, "Index details table not found in response"


        print("Integration test completed successfully!")

        # At minimum, verify that the database connection was properly stored
        # This tests the core state database functionality
        assert db_conn_check, "Core state database functionality failed"

    def test_recommendation_delete(self, server_health_check):
        """
        Test recommendation deletion functionality
        """
        app_url = server_health_check

        # First, create a test recommendation
        test_description = "Test recommendation for deletion"
        test_sql = "SELECT 1 -- Test SQL command"

        # Get a valid pb_id from existing data
        conn = _get_conn()
        cursor = conn.execute("SELECT pbi_id FROM Procedure_blitzindex LIMIT 1")
        row = cursor.fetchone()

        if not row:
            pytest.skip("No Procedure_blitzindex records found for testing")

        pbi_id = row[0]

        # Insert test recommendation
        recommendation_id = dao.insert_recommendation(
            description=test_description,
            sql_command=test_sql,
            pbi_id=pbi_id,
            pb_id=None,
            pbc_id=None
        )

        print(f"Created test recommendation with ID: {recommendation_id}")

        # Verify recommendation exists
        recommendation = dao.get_recommendation(db_conn.get_actual_db_id(), recommendation_id)
        assert recommendation is not None, "Test recommendation was not created"
        assert recommendation.description == test_description

        # Test the delete endpoint via HTTP POST
        delete_url = f"{app_url}/recommendation/{recommendation_id}/delete"

        # Make POST request to delete the recommendation
        response = requests.post(delete_url, timeout=10)

        # Should redirect to recommendations page after successful deletion
        assert response.status_code == 200 or response.status_code == 302, f"Delete request failed with status {response.status_code}"

        # Verify recommendation was actually deleted from database
        deleted_recommendation = dao.get_recommendation(db_conn.get_actual_db_id(), recommendation_id)
        assert deleted_recommendation is None, "Recommendation was not actually deleted from database"

        print(f"✓ Successfully deleted recommendation #{recommendation_id}")


if __name__ == "__main__":
    pytest.main([__file__])
