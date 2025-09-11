import os
import tempfile
import shutil
import pytest
from pydantic import ValidationError
import src.db_DAO as db_dao
from src.db_DAO import DatabaseConnection
from src.connection_DAO import _ensure_db


@pytest.fixture(autouse=True)
def temp_cwd(monkeypatch):
    """Create a temporary directory and switch to it for the test duration"""
    orig_dir = os.getcwd()
    temp_dir = tempfile.mkdtemp()
    monkeypatch.chdir(temp_dir)
    # Initialize the database
    _ensure_db()
    yield
    os.chdir(orig_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_db_connection():
    """Fixture providing a sample database connection"""
    return DatabaseConnection(
        db_name="test_database",
        db_user="test_user",
        db_password="test_password",
        db_host="localhost",
        db_port=5432,
        has_blitz_procedures=True
    )


@pytest.fixture
def another_db_connection():
    """Fixture providing another sample database connection"""
    return DatabaseConnection(
        db_name="another_database",
        db_user="another_user",
        db_password="another_password",
        db_host="remote.example.com",
        db_port=3306,
        has_blitz_procedures=False
    )


class TestDatabaseConnection:
    """Test the DatabaseConnection Pydantic model"""

    def test_valid_database_connection(self):
        """Test creating a valid DatabaseConnection"""
        db_conn = DatabaseConnection(
            db_name="test_db",
            db_user="user",
            db_password="pass",
            db_host="localhost",
            db_port=5432,
            has_blitz_procedures=True
        )
        assert db_conn.db_name == "test_db"
        assert db_conn.db_user == "user"
        assert db_conn.db_password == "pass"
        assert db_conn.db_host == "localhost"
        assert db_conn.db_port == 5432
        assert db_conn.db_id is None
        assert db_conn.has_blitz_procedures is True

    def test_valid_database_connection_without_blitz_procedures(self):
        """Test creating a valid DatabaseConnection without has_blitz_procedures"""
        db_conn = DatabaseConnection(
            db_name="test_db",
            db_user="user",
            db_password="pass",
            db_host="localhost",
            db_port=5432
        )
        assert db_conn.db_name == "test_db"
        assert db_conn.db_user == "user"
        assert db_conn.db_password == "pass"
        assert db_conn.db_host == "localhost"
        assert db_conn.db_port == 5432
        assert db_conn.db_id is None
        assert db_conn.has_blitz_procedures is None

    def test_invalid_port_too_high(self):
        """Test that port validation fails for values > 65535"""
        with pytest.raises(ValidationError):
            DatabaseConnection(
                db_name="test_db",
                db_user="user",
                db_password="pass",
                db_host="localhost",
                db_port=70000,
                has_blitz_procedures=True
            )

    def test_invalid_port_zero(self):
        """Test that port validation fails for zero"""
        with pytest.raises(ValidationError):
            DatabaseConnection(
                db_name="test_db",
                db_user="user",
                db_password="pass",
                db_host="localhost",
                db_port=0,
                has_blitz_procedures=False
            )

    def test_empty_string_fields(self):
        """Test that empty string fields are rejected"""
        with pytest.raises(ValidationError):
            DatabaseConnection(
                db_name="",
                db_user="user",
                db_password="pass",
                db_host="localhost",
                db_port=5432,
                has_blitz_procedures=None
            )


class TestInsertDb:
    """Test the insert_db function"""

    def test_insert_db_success(self, sample_db_connection):
        """Test successful database connection insertion"""
        db_id = db_dao.insert_db(sample_db_connection)
        assert isinstance(db_id, int)
        assert db_id > 0

    def test_insert_db_returns_different_ids(self, sample_db_connection, another_db_connection):
        """Test that multiple insertions return different IDs"""
        db_id1 = db_dao.insert_db(sample_db_connection)
        db_id2 = db_dao.insert_db(another_db_connection)
        assert db_id1 != db_id2

    def test_insert_db_invalid_input(self):
        """Test insert_db with invalid input"""
        with pytest.raises(ValueError):
            db_dao.insert_db("not a DatabaseConnection")


class TestGetDb:
    """Test the get_db function"""

    def test_get_db_success(self, sample_db_connection):
        """Test successful retrieval of database connection"""
        db_id = db_dao.insert_db(sample_db_connection)
        retrieved = db_dao.get_db(db_id)

        assert retrieved is not None
        assert retrieved.db_id == db_id
        assert retrieved.db_name == sample_db_connection.db_name
        assert retrieved.db_user == sample_db_connection.db_user
        assert retrieved.db_password == sample_db_connection.db_password
        assert retrieved.db_host == sample_db_connection.db_host
        assert retrieved.db_port == sample_db_connection.db_port
        assert retrieved.has_blitz_procedures == sample_db_connection.has_blitz_procedures

    def test_get_db_not_found(self):
        """Test get_db with non-existent ID"""
        result = db_dao.get_db(99999)
        assert result is None

    def test_get_db_invalid_id_zero(self):
        """Test get_db with invalid ID (zero)"""
        with pytest.raises(ValueError):
            db_dao.get_db(0)

    def test_get_db_invalid_id_negative(self):
        """Test get_db with invalid ID (negative)"""
        with pytest.raises(ValueError):
            db_dao.get_db(-1)

    def test_get_db_invalid_id_string(self):
        """Test get_db with invalid ID (string)"""
        with pytest.raises(ValueError):
            db_dao.get_db("invalid")

    def test_get_db_with_blitz_procedures_variations(self):
        """Test get_db with different has_blitz_procedures values"""
        # Test with True
        conn_true = DatabaseConnection(
            db_name="test_true",
            db_user="user",
            db_password="pass",
            db_host="localhost",
            db_port=5432,
            has_blitz_procedures=True
        )
        db_id_true = db_dao.insert_db(conn_true)
        retrieved_true = db_dao.get_db(db_id_true)
        assert retrieved_true.has_blitz_procedures is True

        # Test with False
        conn_false = DatabaseConnection(
            db_name="test_false",
            db_user="user",
            db_password="pass",
            db_host="localhost",
            db_port=5433,
            has_blitz_procedures=False
        )
        db_id_false = db_dao.insert_db(conn_false)
        retrieved_false = db_dao.get_db(db_id_false)
        assert retrieved_false.has_blitz_procedures is False

        # Test with None
        conn_none = DatabaseConnection(
            db_name="test_none",
            db_user="user",
            db_password="pass",
            db_host="localhost",
            db_port=5434,
            has_blitz_procedures=None
        )
        db_id_none = db_dao.insert_db(conn_none)
        retrieved_none = db_dao.get_db(db_id_none)
        assert retrieved_none.has_blitz_procedures is None


class TestExistsDb:
    """Test the exists_db function"""

    def test_exists_db_true(self, sample_db_connection):
        """Test exists_db returns db_id for existing connection"""
        db_id = db_dao.insert_db(sample_db_connection)
        result = db_dao.exists_db(
            sample_db_connection.db_host,
            sample_db_connection.db_port,
            sample_db_connection.db_user
        )
        assert result == db_id

    def test_exists_db_false(self):
        """Test exists_db returns -1 for non-existing connection"""
        result = db_dao.exists_db("nonexistent.host", 9999, "nonexistent_user")
        assert result == -1

    def test_exists_db_multiple_same_host_port_different_user(self, sample_db_connection):
        """Test exists_db distinguishes between different users on same host:port"""
        db_dao.insert_db(sample_db_connection)

        # Same host and port, but different user
        result = db_dao.exists_db(
            sample_db_connection.db_host,
            sample_db_connection.db_port,
            "different_user"
        )
        assert result == -1

    def test_exists_db_invalid_host_empty(self):
        """Test exists_db with empty host"""
        with pytest.raises(ValueError):
            db_dao.exists_db("", 5432, "user")

    def test_exists_db_invalid_host_none(self):
        """Test exists_db with None host"""
        with pytest.raises(ValueError):
            db_dao.exists_db(None, 5432, "user")

    def test_exists_db_invalid_port_zero(self):
        """Test exists_db with invalid port (zero)"""
        with pytest.raises(ValueError):
            db_dao.exists_db("localhost", 0, "user")

    def test_exists_db_invalid_port_too_high(self):
        """Test exists_db with invalid port (too high)"""
        with pytest.raises(ValueError):
            db_dao.exists_db("localhost", 70000, "user")

    def test_exists_db_invalid_user_empty(self):
        """Test exists_db with empty user"""
        with pytest.raises(ValueError):
            db_dao.exists_db("localhost", 5432, "")

    def test_exists_db_invalid_user_none(self):
        """Test exists_db with None user"""
        with pytest.raises(ValueError):
            db_dao.exists_db("localhost", 5432, None)


class TestDeleteDb:
    """Test the delete_db function"""

    def test_delete_db_success(self, sample_db_connection):
        """Test successful deletion of database connection"""
        db_id = db_dao.insert_db(sample_db_connection)
        result = db_dao.delete_db(db_id)
        assert result is True

        # Verify it's actually deleted
        retrieved = db_dao.get_db(db_id)
        assert retrieved is None

    def test_delete_db_not_found(self):
        """Test delete_db with non-existent ID"""
        result = db_dao.delete_db(99999)
        assert result is False

    def test_delete_db_invalid_id_zero(self):
        """Test delete_db with invalid ID (zero)"""
        with pytest.raises(ValueError):
            db_dao.delete_db(0)

    def test_delete_db_invalid_id_negative(self):
        """Test delete_db with invalid ID (negative)"""
        with pytest.raises(ValueError):
            db_dao.delete_db(-1)


class TestGetAllDbConnections:
    """Test the get_all_db_connections function"""

    def test_get_all_db_connections_empty(self):
        """Test get_all_db_connections with no connections"""
        connections = db_dao.get_all_db_connections()
        assert connections == []

    def test_get_all_db_connections_single(self, sample_db_connection):
        """Test get_all_db_connections with one connection"""
        db_id = db_dao.insert_db(sample_db_connection)
        connections = db_dao.get_all_db_connections()

        assert len(connections) == 1
        assert connections[0].db_id == db_id
        assert connections[0].db_name == sample_db_connection.db_name

    def test_get_all_db_connections_multiple(self, sample_db_connection, another_db_connection):
        """Test get_all_db_connections with multiple connections"""
        db_id1 = db_dao.insert_db(sample_db_connection)
        db_id2 = db_dao.insert_db(another_db_connection)

        connections = db_dao.get_all_db_connections()
        assert len(connections) == 2

        # Check that they're ordered by db_id
        assert connections[0].db_id == db_id1
        assert connections[1].db_id == db_id2

    def test_get_all_db_connections_after_deletion(self, sample_db_connection, another_db_connection):
        """Test get_all_db_connections after deleting one connection"""
        db_id1 = db_dao.insert_db(sample_db_connection)
        db_id2 = db_dao.insert_db(another_db_connection)

        # Delete first connection
        db_dao.delete_db(db_id1)

        connections = db_dao.get_all_db_connections()
        assert len(connections) == 1
        assert connections[0].db_id == db_id2


class TestIntegration:
    """Integration tests combining multiple functions"""

    def test_full_lifecycle(self, sample_db_connection):
        """Test complete lifecycle: insert, check exists, get, delete"""
        # Insert
        db_id = db_dao.insert_db(sample_db_connection)

        # Check exists
        exists_result = db_dao.exists_db(
            sample_db_connection.db_host,
            sample_db_connection.db_port,
            sample_db_connection.db_user
        )
        assert exists_result == db_id

        # Get
        retrieved = db_dao.get_db(db_id)
        assert retrieved is not None
        assert retrieved.db_name == sample_db_connection.db_name

        # Delete
        deleted = db_dao.delete_db(db_id)
        assert deleted is True

        # Check exists again
        exists_after_delete = db_dao.exists_db(
            sample_db_connection.db_host,
            sample_db_connection.db_port,
            sample_db_connection.db_user
        )
        assert exists_after_delete == -1

    def test_duplicate_connections_allowed(self, sample_db_connection):
        """Test that duplicate connections are allowed (no unique constraint)"""
        db_id1 = db_dao.insert_db(sample_db_connection)
        db_id2 = db_dao.insert_db(sample_db_connection)

        assert db_id1 != db_id2

        # Both should exist
        conn1 = db_dao.get_db(db_id1)
        conn2 = db_dao.get_db(db_id2)

        assert conn1 is not None
        assert conn2 is not None
        assert conn1.db_name == conn2.db_name
        assert conn1.db_host == conn2.db_host
