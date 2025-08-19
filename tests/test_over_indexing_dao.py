import pytest
from unittest.mock import patch
from src.models import BlitzIndexRecord, DBIndexRecord
import src.result_DAO as dao


class TestOverIndexingAnalysis:
    """Test cases for over-indexing analysis functionality"""

    @pytest.fixture
    def sample_blitzindex_record(self):
        """Create a sample BlitzIndex record with over-indexing finding"""
        return BlitzIndexRecord(
            pbi_id=1,
            finding="Over-indexing: 5 or more indexes on dbo.TestTable",
            details_schema_table_index_indexid="dbo.TestTable has 7 indexes",
            priority=100,
            more_info="EXEC dbo.sp_BlitzIndex @DatabaseName='TestDB', @SchemaName='dbo', @TableName='TestTable';",
            raw_record='{"Finding": "Over-indexing", "Details": "Test details", "Priority": 100}',
            procedure_order=1,
            pc_id=1
        )

    @pytest.fixture
    def sample_db_indexes(self):
        """Create sample DB index records"""
        return [
            {
                'db_schema_object_indexid': 'dbo.TestTable.PK_TestTable (1)',
                'index_definition': '[ID] ASC',
                'secret_columns': 'None',
                'fill_factor': 100,
                'index_usage_summary': 'Seeks: 1000, Scans: 10',
                'index_op_stats': 'Updates: 50',
                'index_size_summary': '10 MB',
                'partition_compression_detail': 'None',
                'index_lock_wait_summary': 'No waits',
                'is_referenced_by_foreign_key': 1,
                'fks_covered_by_index': 0,
                'last_user_seek': '2024-08-15 10:00:00',
                'last_user_scan': '2024-08-15 09:00:00',
                'last_user_lookup': None,
                'last_user_update': '2024-08-15 11:00:00',
                'create_date': '2024-01-01 12:00:00',
                'modify_date': '2024-01-01 12:00:00',
                'page_latch_wait_count': 0,
                'page_latch_wait_time': '00:00:00',
                'page_io_latch_wait_count': 0,
                'page_io_latch_wait_time': '00:00:00',
                'create_tsql': 'CREATE UNIQUE CLUSTERED INDEX [PK_TestTable] ON [dbo].[TestTable] ([ID] ASC)',
                'drop_tsql': 'DROP INDEX [PK_TestTable] ON [dbo].[TestTable]'
            },
            {
                'db_schema_object_indexid': 'dbo.TestTable.IX_TestTable_Name (2)',
                'index_definition': '[Name] ASC',
                'secret_columns': 'None',
                'fill_factor': 90,
                'index_usage_summary': 'Seeks: 0, Scans: 0',
                'index_op_stats': 'Updates: 50',
                'index_size_summary': '5 MB',
                'partition_compression_detail': 'None',
                'index_lock_wait_summary': 'No waits',
                'is_referenced_by_foreign_key': 0,
                'fks_covered_by_index': 0,
                'last_user_seek': None,
                'last_user_scan': None,
                'last_user_lookup': None,
                'last_user_update': '2024-08-15 11:00:00',
                'create_date': '2024-01-01 12:00:00',
                'modify_date': '2024-01-01 12:00:00',
                'page_latch_wait_count': 0,
                'page_latch_wait_time': '00:00:00',
                'page_io_latch_wait_count': 0,
                'page_io_latch_wait_time': '00:00:00',
                'create_tsql': 'CREATE NONCLUSTERED INDEX [IX_TestTable_Name] ON [dbo].[TestTable] ([Name] ASC)',
                'drop_tsql': 'DROP INDEX [IX_TestTable_Name] ON [dbo].[TestTable]'
            }
        ]

    def test_store_and_get_db_indexes(self, sample_db_indexes):
        """Test storing and retrieving DB index records"""
        # Convert sample data to DBIndexRecord objects
        index_records = []
        for index_data in sample_db_indexes:
            index_record = DBIndexRecord(pbi_id=1, **index_data)
            index_records.append(index_record)

        # Store indexes
        dao.store_db_indexes(index_records, 1)

        # Retrieve indexes
        retrieved_indexes = dao.get_db_indexes(1)

        # Verify
        assert len(retrieved_indexes) == 2
        assert isinstance(retrieved_indexes[0], DBIndexRecord)
        assert retrieved_indexes[0].pbi_id == 1
        assert retrieved_indexes[0].db_schema_object_indexid == 'dbo.TestTable.PK_TestTable (1)'
        assert retrieved_indexes[1].db_schema_object_indexid == 'dbo.TestTable.IX_TestTable_Name (2)'

    def test_store_db_indexes_replaces_existing(self, sample_db_indexes):
        """Test that storing indexes replaces existing ones for the same pbi_id"""
        # Convert sample data to DBIndexRecord objects
        initial_records = []
        for index_data in sample_db_indexes:
            index_record = DBIndexRecord(pbi_id=1, **index_data)
            initial_records.append(index_record)

        # Store initial indexes
        dao.store_db_indexes(initial_records, 1)

        # Store new indexes for the same pbi_id
        new_index_data = {
            'db_schema_object_indexid': 'dbo.TestTable.IX_NewIndex (3)',
            'index_definition': '[NewColumn] ASC',
            'fill_factor': 100
        }
        new_record = DBIndexRecord(pbi_id=1, **new_index_data)
        dao.store_db_indexes([new_record], 1)

        # Retrieve indexes
        retrieved_indexes = dao.get_db_indexes(1)

        # Verify only the new index exists
        assert len(retrieved_indexes) == 1
        assert retrieved_indexes[0].db_schema_object_indexid == 'dbo.TestTable.IX_NewIndex (3)'

    def test_get_db_indexes_empty_result(self):
        """Test getting indexes when none exist"""
        # Try to get indexes for non-existent pbi_id
        indexes = dao.get_db_indexes(999)

        # Verify empty result
        assert len(indexes) == 0

    @patch('src.agent_blitz_one_blitzindex.execute_more_info_query')
    def test_execute_more_info_query_mock(self, mock_execute):
        """Test the execute_more_info_query function with mock"""
        from src.agent_blitz_one_blitzindex import execute_more_info_query

        # Setup mock return value
        mock_execute.return_value = [
            {'Column1': 'Header1', 'Column2': 'Header2'},  # Q1 row to skip
            {'Column1': 'Value1', 'Column2': 'Value2'}     # Actual data
        ]

        # Call function
        result = execute_more_info_query("EXEC dbo.sp_BlitzIndex @DatabaseName='TestDB'")

        # Verify mock was called
        mock_execute.assert_called_once()
        assert len(result) == 2


    def test_format_index_data_empty(self, sample_blitzindex_record):
        """Test formatting when no index data is available"""
        from src.agent_blitz_one_blitzindex import _format_index_data_for_prompt

        # Format with empty list
        formatted_data = _format_index_data_for_prompt(sample_blitzindex_record, [])

        # Verify fallback message
        assert "No detailed index data available" in formatted_data

    @patch('builtins.open')
    def test_load_over_indexing_prompt(self, mock_open, sample_blitzindex_record):
        """Test loading the over-indexing prompt template"""
        from src.agent_blitz_one_blitzindex import _load_specialized_prompt

        # Mock file content with both finding and index_analysis_data placeholders
        mock_open.return_value.__enter__.return_value.read.return_value = (
            "Test prompt template with {finding} and {index_analysis_data}"
        )

        # Call function with empty indexes
        result = _load_specialized_prompt(sample_blitzindex_record, [], "TestDB")

        # Verify template was loaded and formatted
        # Check that the prompt file was opened (will be among many other file opens)
        prompt_file_opened = any(
            str(call[0][0]).endswith('over_indexing.txt')
            for call in mock_open.call_args_list
        )
        assert prompt_file_opened, "over_indexing.txt prompt file should have been opened"

        assert "Test prompt template with" in result
        assert "Over-indexing: 5 or more indexes on dbo.TestTable" in result
        assert "No detailed index data available" in result
