# Tests

This directory contains the test suite for the blitz_agent project.

## Running Tests

### Prerequisites
Make sure you have your virtual environment activated and all dependencies installed:

```bash
# Activate virtual environment (if not already active)
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate     # On Windows

# Install dependencies (if not already installed)
pip install -r requirements.txt
```

### Running All Tests
From the project root directory:

```bash
# Run all tests
pytest tests/

# Run with verbose output
pytest tests/ -v
```

### Running Specific Test Files
```bash
# Run a specific test file
pytest tests/test_result_DAO.py

# Or using python module
python -m pytest tests/test_result_DAO.py -v
```

### Running Individual Tests
```bash
# Run a specific test function
pytest tests/test_result_DAO.py::test_store_and_get_all_records -v
```

## Integration Tests

### Prerequisites for Integration Tests
Integration tests require a running server instance since they test the complete application flow including HTTP endpoints.

### Running Integration Tests

1. **Start the server first** (in a separate terminal):
   ```bash
   # Make sure you're in the project root directory
   cd /path/to/blitz_agent

   # Activate virtual environment
   source venv/bin/activate

   # Start the Flask server
   python app.py
   ```

2. **Configure environment variables**:
   Make sure your `.env` file contains the `APP_URL` variable:
   ```
   APP_URL=http://localhost:5000
   ```

3. **Run the integration test**:
   ```bash
   # In another terminal, run the integration test
   pytest tests/test_app_integration.py -v
   ```

### Integration Test Features
- **Automatic server health check**: The test will automatically check if the server is running on the specified URL
- **Environment variable configuration**: Uses `APP_URL` from `.env` file (defaults to `http://localhost:5000`)
- **Complete workflow testing**: Tests the entire flow from database initialization to procedure execution
- **Adventure Works integration**: Tests with Adventure Works sample database workload

⚠️ **Important**: Integration tests will be automatically skipped if the server is not running. Make sure to start the server before running these tests.

## Test Descriptions

### test_app_integration.py
**Integration tests** for the complete application workflow including HTTP endpoints and database operations.

#### Prerequisites:
- Running Flask server (automatically checked)
- Environment variables configured in `.env` file
- Adventure Works database setup (optional, handled automatically)

#### Test Functions:

- **`test_adventure_works_integration()`**
  - Complete end-to-end integration test following these steps:
    1. Runs Adventure Works initialization and workload generator
    2. Deletes existing results database
    3. Calls init endpoint to create Database_connection record
    4. Verifies Database_connection content in state database
    5. Calls procedure endpoint with Blitz Index parameter
    6. Verifies that state database contains priority 10 records
    7. Gets rec_id of the priority 10 over-indexing record
    8. Calls analyze endpoint for over-indexing analysis
    9. Verifies that 12 DB_Indexes records are created for Product table
  - **Requires running server**: This test will be automatically skipped if server is not running
  - Tests real HTTP endpoints and database interactions
  - Validates complete application workflow from initialization to analysis
  - **Tests over-indexing analysis**: Specifically tests the `process_over_indexing_analysis` functionality

#### Features:
- **Automatic server detection**: Uses `server_health_check` fixture to verify server availability
- **Environment-based configuration**: Reads `APP_URL` from `.env` file
- **Graceful failure**: Skips tests with clear message if server is not running
- **Real database operations**: Tests actual database state and content verification

### test_result_DAO.py
Tests for the `src.result_DAO` module, which handles database operations for storing and retrieving procedure results and chat history.

#### Test Functions:

- **`test_store_and_get_all_records()`**
  - Tests the basic functionality of storing multiple records and retrieving them
  - Verifies that records are stored correctly and can be retrieved with proper data integrity

- **`test_get_and_store_chat_history()`**
  - Tests the chat history storage and retrieval functionality
  - Ensures chat conversations can be persisted and loaded correctly for specific procedure runs

- **`test_get_record()`**
  - Tests retrieval of individual records by index
  - Verifies that specific records can be accessed from stored procedure results

- **`test_clear_all_and_delete_chat_sessions()`**
  - Tests the cleanup functionality that removes all stored data
  - Ensures that both procedure results and chat history are properly cleared

#### Test Setup
Each test uses a temporary directory fixture (`temp_cwd`) that:
- Creates an isolated temporary directory for each test
- Sets up a clean database environment
- Ensures the required "testproc" procedure type exists
- Cleans up after each test to prevent interference between tests

## Configuration

The project uses a `pyproject.toml` file for pytest configuration, which:
- Sets the Python path to include the project root
- Configures test discovery paths
- Enables verbose output by default

This configuration allows you to run `pytest` directly without path issues.
