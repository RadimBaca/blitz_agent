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

## Test Descriptions

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
