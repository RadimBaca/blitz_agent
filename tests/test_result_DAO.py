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
    # Ensure testproc exists in Procedure_type and create a test database connection
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
        conn.execute(
            "INSERT OR IGNORE INTO Procedure_type (display_name, procedure_name) VALUES (?, ?)",
            ("Test Procedure", "testproc")
        )
        # Create a test database connection
        conn.execute(
            "INSERT OR IGNORE INTO Database_connection (db_id, db_name, db_user, db_password, db_host, db_port) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (1, "test_db", "test_user", "test_password", "localhost", 5432)
        )
        # Create a second test database connection
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

def test_store_and_get_all_records():
    records = [{"foo": "bar"}, {"baz": "qux"}]
    dao.store_records("testproc", records, db_id=1)
    result = dao.get_all_records("testproc", db_id=1)
    assert len(result) == 2
    assert result[0]["foo"] == "bar"
    assert result[1]["baz"] == "qux"

def test_get_and_store_chat_history():
    chat_history = [("user", "hello"), ("ai", "hi")]
    dao.store_records("testproc", [{}], db_id=1)
    dao.store_chat_history("testproc", 0, chat_history)
    loaded = dao.get_chat_history("testproc", 0)
    assert loaded == chat_history

def test_get_record():
    records = [{"foo": "bar"}]
    dao.store_records("testproc", records, db_id=1)
    rec = dao.get_record("testproc", 0)
    assert rec["foo"] == "bar"

def test_clear_all_and_delete_chat_sessions():
    records = [{"foo": "bar"}]
    dao.store_records("testproc", records, db_id=1)
    dao.store_chat_history("testproc", 0, [("user", "hi")])
    # Should remove all kv and chat files
    dao.clear_all(db_id=1)
    assert dao.get_all_records("testproc", db_id=1) == []
    assert dao.get_chat_history("testproc", 0) is None

def test_clear_all_with_multiple_db_ids():
    """Test that clear_all only affects the specified db_id"""
    records1 = [{"foo": "bar1"}]
    records2 = [{"foo": "bar2"}]

    # Store records for both databases
    dao.store_records("testproc", records1, db_id=1)
    dao.store_records("testproc", records2, db_id=2)

    # Store chat history for both
    dao.store_chat_history("testproc", 0, [("user", "hi from db1")])
    # Need to get records for db2 to store cha
    records_db2 = dao.get_all_records("testproc", db_id=2)
    if records_db2:
        dao.store_chat_history("testproc", 0, [("user", "hi from db2")])

    # Clear only db_id=1
    dao.clear_all(db_id=1)

    # Verify db_id=1 is cleared but db_id=2 is intac
    assert dao.get_all_records("testproc", db_id=1) == []
    assert dao.get_all_records("testproc", db_id=2) != []
    assert dao.get_all_records("testproc", db_id=2)[0]["foo"] == "bar2"

def test_delete_results_with_db_id():
    """Test that delete_results only affects the specified db_id"""
    records1 = [{"foo": "bar1"}]
    records2 = [{"foo": "bar2"}]

    # Store records for both databases
    dao.store_records("testproc", records1, db_id=1)
    dao.store_records("testproc", records2, db_id=2)

    # Delete results only for db_id=1
    dao.delete_results("testproc", db_id=1)

    # Verify only db_id=1 results are deleted
    assert dao.get_all_records("testproc", db_id=1) == []
    assert dao.get_all_records("testproc", db_id=2) != []
    assert dao.get_all_records("testproc", db_id=2)[0]["foo"] == "bar2"

def test_delete_chat_sessions_with_db_id():
    """Test that delete_chat_sessions only affects the specified db_id"""
    records1 = [{"foo": "bar1"}]
    records2 = [{"foo": "bar2"}]

    # Store records for both databases
    dao.store_records("testproc", records1, db_id=1)
    dao.store_records("testproc", records2, db_id=2)

    # Store chat history for both
    dao.store_chat_history("testproc", 0, [("user", "hi from db1")])
    # For db2, we need to use its record index
    records_db2 = dao.get_all_records("testproc", db_id=2)
    if records_db2:
        dao.store_chat_history("testproc", 0, [("user", "hi from db2")])

    # Delete chat sessions only for db_id=1
    dao.delete_chat_sessions("testproc", db_id=1)

    # Verify chat history is deleted only for db_id=1
    assert dao.get_chat_history("testproc", 0) is None  # This should be None for db1