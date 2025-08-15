import pytest
import tempfile
import sqlite3
import os
from unittest.mock import patch, MagicMock

# Import the functions to test
import src.result_DAO as dao
from src.models import DBIndexRecord, ChatOverIndexingRecord


class TestOverIndexingFunctions:
    """Test suite for over-indexing related functions in result_DAO.py"""

    @pytest.fixture
    def setup_test_db(self):
        """Setup a temporary test database"""
        # Create a temporary database file
        db_fd, db_path = tempfile.mkstemp()
        os.close(db_fd)

        # Mock the database connection to use our test database
        with patch('src.result_DAO._get_conn') as mock_get_conn:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            mock_get_conn.return_value = conn

            # Create necessary tables
            conn.execute("""
                CREATE TABLE Database_connection (
                    db_id INTEGER PRIMARY KEY,
                    db_name VARCHAR(100) NOT NULL,
                    db_user VARCHAR(100) NOT NULL,
                    db_password VARCHAR(100) NOT NULL,
                    db_host VARCHAR(100) NOT NULL,
                    db_port INTEGER NOT NULL
                )
            """)

            conn.execute("""
                CREATE TABLE Procedure_type (
                    p_id INTEGER PRIMARY KEY,
                    display_name VARCHAR(50),
                    procedure_name VARCHAR(30)
                )
            """)

            conn.execute("""
                CREATE TABLE Procedure_call (
                    pc_id INTEGER PRIMARY KEY,
                    run TIMESTAMP NOT NULL,
                    p_id INTEGER NOT NULL REFERENCES Procedure_type (p_id),
                    db_id INTEGER NOT NULL REFERENCES Database_connection (db_id)
                )
            """)

            conn.execute("""
                CREATE TABLE Procedure_blitzindex (
                    pbi_id INTEGER PRIMARY KEY,
                    procedure_order INTEGER NOT NULL,
                    pc_id INTEGER NOT NULL REFERENCES Procedure_call (pc_id),
                    finding TEXT,
                    details_schema_table_index_indexid TEXT,
                    priority INTEGER,
                    more_info TEXT,
                    raw_record TEXT
                )
            """)

            conn.execute("""
                CREATE TABLE DB_Indexes (
                    di_id INTEGER PRIMARY KEY,
                    pbi_id INTEGER NOT NULL REFERENCES Procedure_blitzindex (pbi_id),
                    db_schema_object_indexid TEXT,
                    index_definition TEXT,
                    secret_columns TEXT,
                    fill_factor INTEGER,
                    index_usage_summary TEXT,
                    index_op_stats TEXT,
                    index_size_summary TEXT,
                    partition_compression_detail TEXT,
                    index_lock_wait_summary TEXT,
                    is_referenced_by_foreign_key INTEGER,
                    fks_covered_by_index INTEGER,
                    last_user_seek TEXT,
                    last_user_scan TEXT,
                    last_user_lookup TEXT,
                    last_user_update TEXT,
                    create_date TEXT,
                    modify_date TEXT,
                    page_latch_wait_count INTEGER,
                    page_latch_wait_time TEXT,
                    page_io_latch_wait_count INTEGER,
                    page_io_latch_wait_time TEXT,
                    create_tsql TEXT,
                    drop_tsql TEXT
                )
            """)

            conn.execute("""
                CREATE TABLE Chat_OverIndexing (
                    coi_id INTEGER PRIMARY KEY,
                    response TEXT NOT NULL,
                    type VARCHAR,
                    chat_order INTEGER
                )
            """)

            # Insert test data
            conn.execute("INSERT INTO Database_connection VALUES (1, 'TestDB', 'user', 'pass', 'localhost', 1433)")
            conn.execute("INSERT INTO Procedure_type VALUES (2, 'Blitz Index', 'sp_BlitzIndex')")
            conn.execute("INSERT INTO Procedure_call VALUES (1, '2024-01-01 10:00:00', 2, 1)")
            conn.execute("""
                INSERT INTO Procedure_blitzindex VALUES (
                    1, 0, 1,
                    'Over-Indexing: Test Table has redundant indexes',
                    'TestDB.dbo.TestTable',
                    100,
                    'EXEC dbo.sp_BlitzIndex @DatabaseName=''TestDB'', @SchemaName=''dbo'', @TableName=''TestTable'';',
                    '{}'
                )
            """)

            conn.commit()

            yield conn, db_path

            conn.close()
            os.unlink(db_path)

    def test_store_db_indexes(self, setup_test_db):
        """Test storing DB indexes data"""
        conn, _ = setup_test_db

        # Test data
        indexes_data = [
            {
                'db_schema_object_indexid': 'TestDB.dbo.TestTable.IX_Test(1)',
                'index_definition': '[NONCLUSTERED] [id] ASC',
                'secret_columns': '',
                'fill_factor': 90,
                'index_usage_summary': 'Reads: 100 Writes: 50',
                'index_op_stats': 'seeks: 80, scans: 20',
                'index_size_summary': '1.2 MB',
                'create_tsql': 'CREATE INDEX IX_Test ON TestTable(id)',
                'drop_tsql': 'DROP INDEX IX_Test ON TestTable'
            },
            {
                'db_schema_object_indexid': 'TestDB.dbo.TestTable.IX_Test2(2)',
                'index_definition': '[NONCLUSTERED] [name] ASC',
                'index_usage_summary': 'Reads: 0 Writes: 0',
                'create_tsql': 'CREATE INDEX IX_Test2 ON TestTable(name)'
            }
        ]

        # Store the data
        dao.store_db_indexes(indexes_data, 1)

        # Verify data was stored
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM DB_Indexes WHERE pbi_id = 1")
        count = cursor.fetchone()[0]
        assert count == 2

        # Verify specific data
        cursor.execute("""
            SELECT db_schema_object_indexid, index_definition, fill_factor
            FROM DB_Indexes
            WHERE pbi_id = 1
            ORDER BY di_id
        """)
        rows = cursor.fetchall()

        assert rows[0][0] == 'TestDB.dbo.TestTable.IX_Test(1)'
        assert rows[0][1] == '[NONCLUSTERED] [id] ASC'
        assert rows[0][2] == 90

        assert rows[1][0] == 'TestDB.dbo.TestTable.IX_Test2(2)'
        assert rows[1][1] == '[NONCLUSTERED] [name] ASC'
        assert rows[1][2] is None  # fill_factor not provided

    def test_get_db_indexes(self, setup_test_db):
        """Test retrieving DB indexes data"""
        conn, _ = setup_test_db

        # First store some test data
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO DB_Indexes (
                pbi_id, db_schema_object_indexid, index_definition,
                fill_factor, index_usage_summary
            ) VALUES (1, 'TestDB.dbo.TestTable.IX_Test(1)',
                     '[NONCLUSTERED] [id] ASC', 90, 'Reads: 100 Writes: 50')
        """)
        conn.commit()

        # Retrieve the data
        indexes = dao.get_db_indexes(1)

        # Verify results
        assert len(indexes) == 1
        assert isinstance(indexes[0], DBIndexRecord)
        assert indexes[0].pbi_id == 1
        assert indexes[0].db_schema_object_indexid == 'TestDB.dbo.TestTable.IX_Test(1)'
        assert indexes[0].index_definition == '[NONCLUSTERED] [id] ASC'
        assert indexes[0].fill_factor == 90
        assert indexes[0].index_usage_summary == 'Reads: 100 Writes: 50'

    def test_store_chat_over_indexing(self, setup_test_db):
        """Test storing over-indexing chat history"""
        conn, _ = setup_test_db

        # Test data
        chat_history = [
            ("user", "Analyze over-indexing issues"),
            ("assistant", "I found 3 redundant indexes that can be safely removed..."),
            ("user", "Can you be more specific about index IX_Test?"),
            ("assistant", "Index IX_Test on column 'id' is redundant because...")
        ]

        # Store the chat history
        dao.store_chat_over_indexing(chat_history)

        # Verify data was stored
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM Chat_OverIndexing")
        count = cursor.fetchone()[0]
        assert count == 4

        # Verify order and content
        cursor.execute("SELECT type, response, chat_order FROM Chat_OverIndexing ORDER BY chat_order")
        rows = cursor.fetchall()

        assert rows[0][0] == "user"
        assert rows[0][1] == "Analyze over-indexing issues"
        assert rows[0][2] == 0

        assert rows[1][0] == "assistant"
        assert "redundant indexes" in rows[1][1]
        assert rows[1][2] == 1

        # Test that storing new chat clears previous chat
        new_chat = [("user", "New analysis")]
        dao.store_chat_over_indexing(new_chat)

        cursor.execute("SELECT COUNT(*) FROM Chat_OverIndexing")
        count = cursor.fetchone()[0]
        assert count == 1

    def test_get_chat_over_indexing(self, setup_test_db):
        """Test retrieving over-indexing chat history"""
        conn, _ = setup_test_db

        # First store some test data
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO Chat_OverIndexing (response, type, chat_order)
            VALUES ('Question about indexes', 'user', 0)
        """)
        cursor.execute("""
            INSERT INTO Chat_OverIndexing (response, type, chat_order)
            VALUES ('Analysis results...', 'assistant', 1)
        """)
        conn.commit()

        # Retrieve the chat history
        chat_history = dao.get_chat_over_indexing()

        # Verify results
        assert chat_history is not None
        assert len(chat_history) == 2
        assert chat_history[0] == ("user", "Question about indexes")
        assert chat_history[1] == ("assistant", "Analysis results...")

        # Test empty chat
        cursor.execute("DELETE FROM Chat_OverIndexing")
        conn.commit()

        empty_chat = dao.get_chat_over_indexing()
        assert empty_chat is None

    def test_get_over_indexing_records(self, setup_test_db):
        """Test retrieving over-indexing BlitzIndex records"""
        conn, _ = setup_test_db

        # Add another test record that should NOT be returned
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO Procedure_blitzindex VALUES (
                2, 1, 1,
                'Missing Index: Add index on column X',
                'TestDB.dbo.TestTable2',
                50,
                'EXEC dbo.sp_BlitzIndex @DatabaseName=''TestDB'', @SchemaName=''dbo'', @TableName=''TestTable2'';',
                '{}'
            )
        """)
        # Add another over-indexing record with higher priority
        cursor.execute("""
            INSERT INTO Procedure_blitzindex VALUES (
                3, 2, 1,
                'Over-Indexing: Another table with redundant indexes',
                'TestDB.dbo.TestTable3',
                200,
                'EXEC dbo.sp_BlitzIndex @DatabaseName=''TestDB'', @SchemaName=''dbo'', @TableName=''TestTable3'';',
                '{}'
            )
        """)
        conn.commit()

        # Test without priority filter - should get both over-indexing records
        records = dao.get_over_indexing_records(1)
        assert len(records) == 2  # Both "Over-Indexing:" records should be returned

        # Test with priority filter - should get only the record with priority <= 150
        records = dao.get_over_indexing_records(1, max_priority=150)
        assert len(records) == 1  # Only the record with priority 100 should be returned
        assert records[0].pbi_id == 1
        assert records[0].finding == 'Over-Indexing: Test Table has redundant indexes'
        assert records[0].priority == 100

        # Test with very low priority filter - should get no records
        records = dao.get_over_indexing_records(1, max_priority=50)
        assert len(records) == 0

        # Test with very high priority filter - should get both records
        records = dao.get_over_indexing_records(1, max_priority=300)
        assert len(records) == 2

        # Verify specific data from first record
        records = dao.get_over_indexing_records(1)
        first_record = next(r for r in records if r.pbi_id == 1)
        assert first_record.details_schema_table_index_indexid == 'TestDB.dbo.TestTable'
        assert first_record.priority == 100
        assert 'EXEC dbo.sp_BlitzIndex' in first_record.more_info

        # Test with non-existent db_id
        with pytest.raises(ValueError, match="Database connection with db_id '999' does not exist"):
            dao.get_over_indexing_records(999)

    def test_db_index_record_validation(self):
        """Test DBIndexRecord model validation"""
        # Test valid record
        valid_data = {
            'pbi_id': 1,
            'db_schema_object_indexid': 'TestDB.dbo.TestTable.IX_Test(1)',
            'index_definition': '[NONCLUSTERED] [id] ASC',
            'fill_factor': 90
        }

        record = DBIndexRecord(**valid_data)
        assert record.pbi_id == 1
        assert record.fill_factor == 90
        assert record.di_id is None  # Optional field

        # Test missing required field
        invalid_data = {
            'db_schema_object_indexid': 'TestDB.dbo.TestTable.IX_Test(1)',
            'index_definition': '[NONCLUSTERED] [id] ASC'
        }

        with pytest.raises(ValueError):
            DBIndexRecord(**invalid_data)

    def test_chat_over_indexing_record_validation(self):
        """Test ChatOverIndexingRecord model validation"""
        # Test valid record
        valid_data = {
            'response': 'Analysis complete',
            'type': 'assistant',
            'chat_order': 1
        }

        record = ChatOverIndexingRecord(**valid_data)
        assert record.response == 'Analysis complete'
        assert record.type == 'assistant'
        assert record.chat_order == 1
        assert record.coi_id is None  # Optional field

        # Test missing required field
        invalid_data = {
            'type': 'user',
            'chat_order': 0
        }

        with pytest.raises(ValueError):
            ChatOverIndexingRecord(**invalid_data)
