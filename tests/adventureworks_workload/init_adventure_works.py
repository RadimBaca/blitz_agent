#!/usr/bin/env python3
"""
AdventureWorks2019 Over-Indexing Simulation Script
This script creates strategic indexes on Production.Product table to simulate over-indexing scenarios.

Existing indexes on Production.Product:
- PK_Product_ProductID (ProductID) - Primary Key
- AK_Product_Name (Name) - Unique
- AK_Product_ProductNumber (ProductNumber) - Unique
- AK_Product_rowguid (rowguid) - Unique

New indexes created:
Set 1:
1. IX_Product_DaysToManufacture - Never used index
2. IX_Product_SubcategoryID_ListPrice_FinishedGoods - Compound index for common queries
3. IX_Product_ListPrice_Included - Covering index with included columns
4. IX_Product_FinishedGoodsFlag - Made redundant by index #2

Set 2:
5. IX_Product_SellStartDate - Never used index
6. IX_Product_Color_SafetyStock_ReorderPoint - Compound index for inventory queries
7. IX_Product_Color_Included - Covering index for color-based searches
8. IX_Product_SafetyStockLevel - Made redundant by index #6
"""

import os
import pyodbc
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from the tests directory
test_dir = Path(__file__).parent
load_dotenv(test_dir / '.env')

def get_connection():
    """Create database connection using test environment variables"""
    connection_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('MOCK_MSSQL_HOST')},{os.getenv('MOCK_MSSQL_PORT')};"
        f"DATABASE={os.getenv('MOCK_MSSQL_DB')};"
        f"UID={os.getenv('MOCK_MSSQL_USER')};"
        f"PWD={os.getenv('MOCK_MSSQL_PASSWORD')};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(connection_string)

def index_exists(cursor, index_name, table_name='Production.Product'):
    """Check if an index exists on the specified table"""
    check_query = """
    SELECT COUNT(*)
    FROM sys.indexes i
    INNER JOIN sys.objects o ON i.object_id = o.object_id
    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
    WHERE s.name + '.' + o.name = ?
    AND i.name = ?
    """
    cursor.execute(check_query, table_name, index_name)
    return cursor.fetchone()[0] > 0

def create_over_indexing_scenario():
    """Create indexes to simulate over-indexing on Production.Product table"""

    # Index definitions
    indexes = [
        {
            'name': 'IX_Product_DaysToManufacture',
            'description': 'Index #1: Never used - DaysToManufacture is rarely queried',
            'sql': """
            CREATE NONCLUSTERED INDEX IX_Product_DaysToManufacture
            ON Production.Product (DaysToManufacture ASC)
            """,
            'justification': 'This index will never be used because DaysToManufacture is not used in WHERE, ORDER BY, or JOIN clauses in the workload'
        },

        {
            'name': 'IX_Product_SubcategoryID_ListPrice_FinishedGoods',
            'description': 'Index #2: Compound index for common query pattern',
            'sql': """
            CREATE NONCLUSTERED INDEX IX_Product_SubcategoryID_ListPrice_FinishedGoods
            ON Production.Product (ProductSubcategoryID ASC, ListPrice ASC, FinishedGoodsFlag ASC)
            """,
            'justification': 'This index supports the common query: WHERE ProductSubcategoryID = X AND ListPrice BETWEEN Y AND Z AND FinishedGoodsFlag = 1'
        },

        {
            'name': 'IX_Product_ListPrice_Included',
            'description': 'Index #3: Covering index for product search queries',
            'sql': """
            CREATE NONCLUSTERED INDEX IX_Product_ListPrice_Included
            ON Production.Product (ListPrice ASC)
            INCLUDE (ProductID, Name, ProductNumber, Color, FinishedGoodsFlag, ProductSubcategoryID)
            """,
            'justification': 'This covering index supports product searches by price with all commonly selected columns included'
        },

        {
            'name': 'IX_Product_FinishedGoodsFlag',
            'description': 'Index #4: Redundant index made obsolete by compound index #2',
            'sql': """
            CREATE NONCLUSTERED INDEX IX_Product_FinishedGoodsFlag
            ON Production.Product (FinishedGoodsFlag ASC)
            """,
            'justification': 'This index is redundant because compound index #2 has FinishedGoodsFlag as the third column and can satisfy queries filtering only on FinishedGoodsFlag'
        },

        # Second set of indexes following the same pattern
        {
            'name': 'IX_Product_SellStartDate',
            'description': 'Index #5: Never used - SellStartDate is rarely queried alone',
            'sql': """
            CREATE NONCLUSTERED INDEX IX_Product_SellStartDate
            ON Production.Product (SellStartDate ASC)
            """,
            'justification': 'This index will never be used because SellStartDate is not used in WHERE, ORDER BY, or JOIN clauses in the workload'
        },

        {
            'name': 'IX_Product_Color_SafetyStock_ReorderPoint',
            'description': 'Index #6: Compound index for inventory management queries',
            'sql': """
            CREATE NONCLUSTERED INDEX IX_Product_Color_SafetyStock_ReorderPoint
            ON Production.Product (Color ASC, SafetyStockLevel ASC, ReorderPoint ASC)
            """,
            'justification': 'This index supports inventory queries filtering by Color and safety stock levels for warehouse management'
        },

        {
            'name': 'IX_Product_Color_Included',
            'description': 'Index #7: Covering index for color-based product searches',
            'sql': """
            CREATE NONCLUSTERED INDEX IX_Product_Color_Included
            ON Production.Product (Color ASC)
            INCLUDE (ProductID, Name, ProductNumber, ListPrice, SafetyStockLevel, ReorderPoint)
            """,
            'justification': 'This covering index supports product searches by color with inventory-related columns included'
        },

        {
            'name': 'IX_Product_SafetyStockLevel',
            'description': 'Index #8: Redundant index made obsolete by compound index #6',
            'sql': """
            CREATE NONCLUSTERED INDEX IX_Product_SafetyStockLevel
            ON Production.Product (SafetyStockLevel ASC)
            """,
            'justification': 'This index is redundant because compound index #6 has SafetyStockLevel as the second column and can satisfy queries filtering only on SafetyStockLevel'
        }
    ]

    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            print("Initializing AdventureWorks over-indexing simulation...")
            print("=" * 60)

            for idx in indexes:
                print(f"\n{idx['description']}")
                print(f"Index Name: {idx['name']}")
                print(f"Justification: {idx['justification']}")

                if index_exists(cursor, idx['name']):
                    print(f"✓ Index {idx['name']} already exists - skipping")
                else:
                    try:
                        cursor.execute(idx['sql'])
                        conn.commit()
                        print(f"✓ Successfully created index {idx['name']}")
                    except pyodbc.Error as e:
                        print(f"✗ Failed to create index {idx['name']}: {e}")
                        conn.rollback()

            print("\n" + "=" * 60)
            print("Over-indexing simulation setup complete!")

            # Display current indexes on Production.Product
            print("\nCurrent indexes on Production.Product:")
            cursor.execute("""
                SELECT
                    i.name AS IndexName,
                    i.type_desc AS IndexType,
                    STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS KeyColumns
                FROM sys.indexes i
                INNER JOIN sys.objects o ON i.object_id = o.object_id
                INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                LEFT JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id AND ic.is_included_column = 0
                LEFT JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE s.name = 'Production' AND o.name = 'Product'
                AND i.type IN (1, 2)  -- Clustered and Non-clustered
                GROUP BY i.name, i.type_desc, i.index_id
                ORDER BY i.index_id
            """)

            indexes_list = cursor.fetchall()
            for idx in indexes_list:
                print(f"  - {idx.IndexName} ({idx.IndexType}): {idx.KeyColumns}")

    except pyodbc.Error as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise

def verify_over_indexing_scenario():
    """Verify the over-indexing scenario by showing index usage statistics"""

    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            print("\nIndex Usage Statistics for Production.Product:")
            print("=" * 60)

            # Query to show index usage statistics
            cursor.execute("""
                SELECT
                    i.name AS IndexName,
                    i.type_desc AS IndexType,
                    ius.user_seeks,
                    ius.user_scans,
                    ius.user_lookups,
                    ius.user_updates,
                    CASE
                        WHEN ius.user_seeks + ius.user_scans + ius.user_lookups = 0 THEN 'UNUSED'
                        ELSE 'USED'
                    END AS UsageStatus
                FROM sys.indexes i
                INNER JOIN sys.objects o ON i.object_id = o.object_id
                INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                LEFT JOIN sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id
                    AND i.index_id = ius.index_id
                    AND ius.database_id = DB_ID()
                WHERE s.name = 'Production' AND o.name = 'Product'
                AND i.type IN (1, 2)  -- Clustered and Non-clustered
                ORDER BY i.index_id
            """)

            stats = cursor.fetchall()
            for stat in stats:
                total_reads = (stat.user_seeks or 0) + (stat.user_scans or 0) + (stat.user_lookups or 0)
                print(f"  {stat.IndexName}:")
                print(f"    Type: {stat.IndexType}")
                print(f"    Reads: {total_reads} (Seeks: {stat.user_seeks or 0}, Scans: {stat.user_scans or 0}, Lookups: {stat.user_lookups or 0})")
                print(f"    Updates: {stat.user_updates or 0}")
                print(f"    Status: {stat.UsageStatus}")
                print()

    except pyodbc.Error as e:
        print(f"Database error: {e}")
        raise

def cleanup_over_indexing_scenario():
    """Remove the created indexes to clean up the over-indexing scenario"""

    indexes_to_drop = [
        # Set 1
        'IX_Product_DaysToManufacture',
        'IX_Product_SubcategoryID_ListPrice_FinishedGoods',
        'IX_Product_ListPrice_Included',
        'IX_Product_FinishedGoodsFlag',
        # Set 2
        'IX_Product_SellStartDate',
        'IX_Product_Color_SafetyStock_ReorderPoint',
        'IX_Product_Color_Included',
        'IX_Product_SafetyStockLevel'
    ]

    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            print("Cleaning up over-indexing simulation...")
            print("=" * 40)

            for index_name in indexes_to_drop:
                if index_exists(cursor, index_name):
                    try:
                        cursor.execute(f"DROP INDEX {index_name} ON Production.Product")
                        conn.commit()
                        print(f"✓ Dropped index {index_name}")
                    except pyodbc.Error as e:
                        print(f"✗ Failed to drop index {index_name}: {e}")
                        conn.rollback()
                else:
                    print(f"- Index {index_name} does not exist - skipping")

            print("\nCleanup complete!")

    except pyodbc.Error as e:
        print(f"Database error: {e}")
        raise

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        if sys.argv[1] == "cleanup":
            cleanup_over_indexing_scenario()
        elif sys.argv[1] == "verify":
            verify_over_indexing_scenario()
        elif sys.argv[1] == "init":
            create_over_indexing_scenario()
        else:
            print("Usage: python init_adventure_works.py [init|verify|cleanup]")
            print("  init    - Create over-indexing scenario (default)")
            print("  verify  - Show index usage statistics")
            print("  cleanup - Remove created indexes")
    else:
        create_over_indexing_scenario()
