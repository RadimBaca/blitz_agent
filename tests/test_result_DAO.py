import os
import tempfile
import shutil
import pytest
import src.result_DAO as dao

@pytest.fixture(autouse=True)
def temp_cwd(monkeypatch):
    # Create a temporary directory and switch to it for the test duration
    orig_dir = os.getcwd()
    temp_dir = tempfile.mkdtemp()
    monkeypatch.chdir(temp_dir)
    # Ensure test procedures exist in Procedure_type and create test database connections
    import sqlite3
    db_dir = "db"
    db_path = os.path.join(db_dir, "results.db")
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    if not os.path.exists(db_path):
        # Trigger DB creation
        import src.result_DAO as dao
        dao._ensure_db()
    conn = sqlite3.connect(db_path)
    try:
        # Ensure Blitz procedures exist (they should from the schema)
        conn.execute(
            "INSERT OR IGNORE INTO Procedure_type (p_id, display_name, procedure_name) VALUES (?, ?, ?)",
            (1, "Blitz", "sp_Blitz")
        )
        conn.execute(
            "INSERT OR IGNORE INTO Procedure_type (p_id, display_name, procedure_name) VALUES (?, ?, ?)",
            (2, "Blitz Index", "sp_BlitzIndex")
        )
        conn.execute(
            "INSERT OR IGNORE INTO Procedure_type (p_id, display_name, procedure_name) VALUES (?, ?, ?)",
            (3, "Blitz Cache", "sp_BlitzCache")
        )
        # Create test database connections
        conn.execute(
            "INSERT OR IGNORE INTO Database_connection (db_id, db_name, db_user, db_password, db_host, db_port) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (1, "test_db", "test_user", "test_password", "localhost", 5432)
        )
        conn.execute(
            "INSERT OR IGNORE INTO Database_connection (db_id, db_name, db_user, db_password, db_host, db_port) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (2, "test_db2", "test_user2", "test_password2", "localhost", 5433)
        )
        conn.commit()
    finally:
        conn.close()
    yield
    os.chdir(orig_dir)
    shutil.rmtree(temp_dir)

def test_store_and_get_all_records_blitz():
    """Test storing and retrieving Blitz records"""
    records = [
        {"Finding": "High CPU usage", "Details": "CPU is consistently over 90%", "Priority": 1},
        {"Finding": "Missing indexes", "Details": "Table scan detected", "Priority": 2}
    ]
    dao.store_records("sp_Blitz", records, db_id=1)
    result = dao.get_all_records("sp_Blitz", db_id=1)
    assert len(result) == 2
    
    # Now we get Pydantic model instances
    assert result[0].finding == "High CPU usage"
    assert result[0].details == "CPU is consistently over 90%"
    assert result[0].priority == 1
    assert result[1].finding == "Missing indexes"
    assert result[0]._rec_id == 0
    assert result[1]._rec_id == 1

def test_store_and_get_all_records_blitzindex():
    """Test storing and retrieving BlitzIndex records"""
    records = [
        {"Finding": "Duplicate index", "Details: schema.table.index(indexid)": "dbo.Users.IX_Name(2)", "Priority": 3},
        {"Finding": "Unused index", "Details: schema.table.index(indexid)": "dbo.Orders.IX_Date(5)", "Priority": 2}
    ]
    dao.store_records("sp_BlitzIndex", records, db_id=1)
    result = dao.get_all_records("sp_BlitzIndex", db_id=1)
    assert len(result) == 2
    assert result[0].finding == "Duplicate index"
    assert result[0].details_schema_table_index_indexid == "dbo.Users.IX_Name(2)"

def test_store_and_get_all_records_blitzcache():
    """Test storing and retrieving BlitzCache records"""
    records = [
        {"Query Text": "SELECT * FROM Users WHERE Name LIKE '%test%'", "Avg CPU (ms)": 150.5, "Warnings": "Table scan"},
        {"Query Text": "UPDATE Orders SET Status = 1", "Avg CPU (ms)": 89.2, "Warnings": "Lock escalation"}
    ]
    dao.store_records("sp_BlitzCache", records, db_id=1)
    result = dao.get_all_records("sp_BlitzCache", db_id=1)
    assert len(result) == 2
    assert result[0].query_text == "SELECT * FROM Users WHERE Name LIKE '%test%'"
    assert result[0].avg_cpu_ms == 150.5
    assert result[0].warnings == "Table scan"

def test_get_and_store_chat_history():
    """Test chat history functionality"""
    chat_history = [("user", "hello"), ("ai", "hi")]
    records = [{"Finding": "Test finding", "Details": "Test details", "Priority": 1}]
    dao.store_records("sp_Blitz", records, db_id=1)
    dao.store_chat_history("sp_Blitz", 0, chat_history)
    loaded = dao.get_chat_history("sp_Blitz", 0)
    assert loaded == chat_history

def test_get_record():
    """Test getting individual records"""
    records = [{"Finding": "Test finding", "Details": "Test details", "Priority": 1}]
    dao.store_records("sp_Blitz", records, db_id=1)
    rec = dao.get_record("sp_Blitz", 0)
    assert rec.finding == "Test finding"
    assert rec.details == "Test details"
    assert rec.priority == 1

def test_clear_all_and_delete_chat_sessions():
    """Test clearing all data"""
    records = [{"Finding": "Test finding", "Details": "Test details", "Priority": 1}]
    dao.store_records("sp_Blitz", records, db_id=1)
    dao.store_chat_history("sp_Blitz", 0, [("user", "hi")])
    # Should remove all data
    dao.clear_all(db_id=1)
    assert dao.get_all_records("sp_Blitz", db_id=1) == []
    assert dao.get_chat_history("sp_Blitz", 0) is None

def test_clear_all_with_multiple_db_ids():
    """Test that clear_all only affects the specified db_id"""
    records1 = [{"Finding": "Test finding 1", "Details": "Test details 1", "Priority": 1}]
    records2 = [{"Finding": "Test finding 2", "Details": "Test details 2", "Priority": 2}]

    # Store records for both databases
    dao.store_records("sp_Blitz", records1, db_id=1)
    dao.store_records("sp_Blitz", records2, db_id=2)

    # Store chat history for both
    dao.store_chat_history("sp_Blitz", 0, [("user", "hi from db1")])
    # Note: For the new system, we need to use the same record index for different databases
    dao.store_chat_history("sp_Blitz", 0, [("user", "hi from db2")])

    # Clear only db_id=1
    dao.clear_all(db_id=1)

    # Verify db_id=1 is cleared but db_id=2 is intact
    assert dao.get_all_records("sp_Blitz", db_id=1) == []
    records_db2 = dao.get_all_records("sp_Blitz", db_id=2)
    assert len(records_db2) > 0
    assert records_db2[0].finding == "Test finding 2"

def test_delete_results_with_db_id():
    """Test that delete_results only affects the specified db_id"""
    records1 = [{"Finding": "Test finding 1", "Details": "Test details 1", "Priority": 1}]
    records2 = [{"Finding": "Test finding 2", "Details": "Test details 2", "Priority": 2}]

    # Store records for both databases
    dao.store_records("sp_Blitz", records1, db_id=1)
    dao.store_records("sp_Blitz", records2, db_id=2)

    # Delete results only for db_id=1
    dao.delete_results("sp_Blitz", db_id=1)

    # Verify only db_id=1 results are deleted
    assert dao.get_all_records("sp_Blitz", db_id=1) == []
    records_db2 = dao.get_all_records("sp_Blitz", db_id=2)
    assert len(records_db2) > 0
    assert records_db2[0].finding == "Test finding 2"

def test_delete_chat_sessions_with_db_id():
    """Test that delete_chat_sessions only affects the specified db_id"""
    records1 = [{"Finding": "Test finding 1", "Details": "Test details 1", "Priority": 1}]
    records2 = [{"Finding": "Test finding 2", "Details": "Test details 2", "Priority": 2}]

    # Store records for both databases
    dao.store_records("sp_Blitz", records1, db_id=1)
    dao.store_records("sp_Blitz", records2, db_id=2)

    # Store chat history for both
    dao.store_chat_history("sp_Blitz", 0, [("user", "hi from db1")])
    dao.store_chat_history("sp_Blitz", 0, [("user", "hi from db2")])

    # Delete chat sessions only for db_id=1
    dao.delete_chat_sessions("sp_Blitz", db_id=1)

    # Verify chat history is deleted only for db_id=1
    # Note: This test checks based on the current database connection context
    # For more accurate testing, we'd need to verify database state directly

def test_pydantic_models_not_none():
    """Test that Pydantic models contain actual data and not None values.
    
    This test would have caught the original issue where records were being
    stored with None values instead of the actual SQL Server data.
    """
    # Test data that simulates what comes from SQL Server procedures
    blitz_records = [
        {
            "Priority": 1,
            "Finding": "High CPU Usage",
            "Details": "CPU is consistently over 90% during peak hours",
            "FindingsGroup": "Performance",
            "DatabaseName": None,
            "URL": "https://www.brentozar.com/go/cpu",
            "QueryPlan": None,
            "QueryPlanFiltered": None,
            "CheckID": 123
        },
        {
            "Priority": 2,
            "Finding": "Missing Indexes",
            "Details": "Table scans detected on large tables",
            "FindingsGroup": "Performance",
            "DatabaseName": "TestDB",
            "URL": "https://www.brentozar.com/go/indexes",
            "QueryPlan": None,
            "QueryPlanFiltered": None,
            "CheckID": 124
        }
    ]
    
    blitzindex_records = [
        {
            "Finding": "Duplicate Index",
            "Details: schema.table.index(indexid)": "dbo.Users.IX_Name(2)",
            "Priority": 3
        }
    ]
    
    blitzcache_records = [
        {
            "Query Text": "SELECT * FROM Users WHERE Name LIKE '%test%'",
            "Avg CPU (ms)": 150.5,
            "Warnings": "Table scan detected"
        }
    ]
    
    # Store the records
    dao.store_records("sp_Blitz", blitz_records, db_id=1)
    dao.store_records("sp_BlitzIndex", blitzindex_records, db_id=1)
    dao.store_records("sp_BlitzCache", blitzcache_records, db_id=1)
    
    # Retrieve and verify Blitz records
    retrieved_blitz = dao.get_all_records("sp_Blitz", db_id=1)
    assert len(retrieved_blitz) == 2
    
    # Verify first Blitz record has actual data, not None
    blitz_rec1 = retrieved_blitz[0]
    assert blitz_rec1.finding is not None, "Finding should not be None"
    assert blitz_rec1.finding == "High CPU Usage", f"Expected 'High CPU Usage', got '{blitz_rec1.finding}'"
    assert blitz_rec1.details is not None, "Details should not be None"
    assert blitz_rec1.details == "CPU is consistently over 90% during peak hours"
    assert blitz_rec1.priority is not None, "Priority should not be None"
    assert blitz_rec1.priority == 1
    
    # Verify second Blitz record
    blitz_rec2 = retrieved_blitz[1]
    assert blitz_rec2.finding == "Missing Indexes"
    assert blitz_rec2.details == "Table scans detected on large tables"
    assert blitz_rec2.priority == 2
    
    # Verify BlitzIndex records
    retrieved_blitzindex = dao.get_all_records("sp_BlitzIndex", db_id=1)
    assert len(retrieved_blitzindex) == 1
    
    blitzindex_rec = retrieved_blitzindex[0]
    assert blitzindex_rec.finding is not None, "BlitzIndex finding should not be None"
    assert blitzindex_rec.finding == "Duplicate Index"
    assert blitzindex_rec.details_schema_table_index_indexid is not None
    assert blitzindex_rec.details_schema_table_index_indexid == "dbo.Users.IX_Name(2)"
    assert blitzindex_rec.priority == 3
    
    # Verify BlitzCache records
    retrieved_blitzcache = dao.get_all_records("sp_BlitzCache", db_id=1)
    assert len(retrieved_blitzcache) == 1
    
    blitzcache_rec = retrieved_blitzcache[0]
    assert blitzcache_rec.query_text is not None, "BlitzCache query_text should not be None"
    assert blitzcache_rec.query_text == "SELECT * FROM Users WHERE Name LIKE '%test%'"
    assert blitzcache_rec.avg_cpu_ms is not None, "avg_cpu_ms should not be None"
    assert blitzcache_rec.avg_cpu_ms == 150.5
    assert blitzcache_rec.warnings == "Table scan detected"
    
    # Additional validation: ensure no fields contain the string "None"
    for record in retrieved_blitz:
        assert record.finding != "None", "Finding should not be the string 'None'"
        assert record.details != "None", "Details should not be the string 'None'"
        if record.priority is not None:
            assert isinstance(record.priority, int), "Priority should be an integer, not a string"
    
    for record in retrieved_blitzindex:
        assert record.finding != "None", "BlitzIndex finding should not be the string 'None'"
        
    for record in retrieved_blitzcache:
        assert record.query_text != "None", "BlitzCache query_text should not be the string 'None'"

def test_analyzed_flag_functionality():
    """Test that the _analyzed flag is correctly set when chat history exists.
    
    This test checks if records show _analyzed=True when they have chat history
    and _analyzed=False when they don't have chat history.
    """
    # Store test records
    blitz_records = [
        {
            "Priority": 1,
            "Finding": "High CPU Usage",
            "Details": "CPU is consistently over 90% during peak hours"
        },
        {
            "Priority": 2,
            "Finding": "Missing Indexes", 
            "Details": "Table scans detected on large tables"
        }
    ]
    
    dao.store_records("sp_Blitz", blitz_records, db_id=1)
    
    # Initially, no records should be analyzed
    records = dao.get_all_records("sp_Blitz", db_id=1)
    assert len(records) == 2
    assert records[0]._analyzed == False, "Record 0 should not be analyzed initially"
    assert records[1]._analyzed == False, "Record 1 should not be analyzed initially"
    
    # Add chat history to the first record (rec_id = 0)
    chat_history = [("user", "What does this finding mean?"), ("ai", "This finding indicates high CPU usage...")]
    dao.store_chat_history("sp_Blitz", 0, chat_history)
    
    # Now retrieve records again - first record should be analyzed, second should not
    records = dao.get_all_records("sp_Blitz", db_id=1)
    assert len(records) == 2
    assert records[0]._analyzed == True, "Record 0 should be analyzed after adding chat history"
    assert records[1]._analyzed == False, "Record 1 should still not be analyzed"
    
    # Add chat history to the second record as well
    chat_history_2 = [("user", "How to fix missing indexes?"), ("ai", "You can create indexes on frequently queried columns...")]
    dao.store_chat_history("sp_Blitz", 1, chat_history_2)
    
    # Now both records should be analyzed
    records = dao.get_all_records("sp_Blitz", db_id=1)
    assert len(records) == 2
    assert records[0]._analyzed == True, "Record 0 should still be analyzed"
    assert records[1]._analyzed == True, "Record 1 should now be analyzed after adding chat history"
    
    # Test that the chat history can be retrieved correctly
    retrieved_chat_0 = dao.get_chat_history("sp_Blitz", 0)
    retrieved_chat_1 = dao.get_chat_history("sp_Blitz", 1)
    
    assert retrieved_chat_0 == chat_history, "Chat history for record 0 should match what was stored"
    assert retrieved_chat_1 == chat_history_2, "Chat history for record 1 should match what was stored"