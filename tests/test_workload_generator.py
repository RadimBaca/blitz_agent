#!/usr/bin/env python3
"""
Simple test script to verify the workload generator functions work correctly
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from workload_generator import get_connection, get_sample_data, run_workload_queries

def test_connection():
    """Test database connection"""
    print("Testing database connection...")
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION, DB_NAME()")
            version, db_name = cursor.fetchone()
            print(f"✓ Connected to database: {db_name}")
            return True
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False

def test_sample_data():
    """Test sample data retrieval"""
    print("Testing sample data retrieval...")
    try:
        sample_data = get_sample_data()
        print(f"✓ Retrieved sample data:")
        for key, values in sample_data.items():
            print(f"  - {key}: {len(values)} items")
        return sample_data
    except Exception as e:
        print(f"✗ Sample data retrieval failed: {e}")
        return None

def test_queries(sample_data):
    """Test query generation"""
    print("Testing query generation...")
    try:
        queries = run_workload_queries(sample_data)
        print(f"✓ Generated {len(queries)} queries:")
        for query in queries:
            print(f"  - {query['name']} (weight: {query['weight']})")
        return True
    except Exception as e:
        print(f"✗ Query generation failed: {e}")
        return False

def test_sample_query_execution(sample_data):
    """Test executing one sample query"""
    print("Testing sample query execution...")
    try:
        queries = run_workload_queries(sample_data)
        if queries:
            test_query = queries[0]  # Test first query
            with get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(test_query['query'])
                rows = cursor.fetchall()
                print(f"✓ Executed '{test_query['name']}' - returned {len(rows)} rows")
                return True
    except Exception as e:
        print(f"✗ Query execution failed: {e}")
        return False

def main():
    """Run all tests"""
    print("AdventureWorks2019 Workload Generator - Quick Test")
    print("=" * 50)

    # Test connection
    if not test_connection():
        return False

    # Test sample data
    sample_data = test_sample_data()
    if not sample_data:
        return False

    # Test query generation
    if not test_queries(sample_data):
        return False

    # Test query execution
    if not test_sample_query_execution(sample_data):
        return False

    print("\n✓ All tests passed! Workload generator is ready to use.")
    print("\nTo run the full workload generator:")
    print("  python tests/workload_generator.py")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
