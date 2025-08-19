#!/usr/bin/env python3
"""
AdventureWorks2019 Over-Indexing and Heap Table Simulation Script

This script provides two main functionalities:
1. Creates strategic indexes on Production.Product table to simulate over-indexing scenarios
2. Converts Sales.SalesOrderDetail table to heap table by dropping clustered index

Production.Product Over-Indexing:
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

Sales.SalesOrderDetail Heap Conversion:
- Creates non-clustered index with same key as existing clustered index
- Drops the clustered index to convert table to heap
- Provides performance degradation scenario for heap table analysis
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


def is_heap_table(cursor, table_name='Sales.SalesOrderDetail'):
    """Check if a table is a heap (has no clustered index)"""
    check_query = """
    SELECT COUNT(*)
    FROM sys.indexes i
    INNER JOIN sys.objects o ON i.object_id = o.object_id
    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
    WHERE s.name + '.' + o.name = ?
    AND i.type = 1  -- Clustered index
    """
    cursor.execute(check_query, table_name)
    return cursor.fetchone()[0] == 0


def get_clustered_index_info(cursor, table_name='Sales.SalesOrderDetail'):
    """Get clustered index information for the table"""
    query = """
    SELECT
        i.name AS IndexName,
        STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS KeyColumns,
        CASE WHEN i.is_primary_key = 1 THEN 1 ELSE 0 END AS IsPrimaryKey,
        CASE WHEN i.is_primary_key = 1 THEN
            (SELECT kc.name
             FROM sys.key_constraints kc
             WHERE kc.parent_object_id = i.object_id
             AND kc.type = 'PK')
        ELSE NULL END AS ConstraintName
    FROM sys.indexes i
    INNER JOIN sys.objects o ON i.object_id = o.object_id
    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id AND ic.is_included_column = 0
    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE s.name + '.' + o.name = ?
    AND i.type = 1  -- Clustered index
    GROUP BY i.name, i.is_primary_key, i.object_id
    """
    cursor.execute(query, table_name)
    return cursor.fetchone()

def create_heap_table_scenario():
    """Convert Sales.SalesOrderDetail to heap table by creating NC index and dropping clustered index"""

    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            print("Initializing Sales.SalesOrderDetail heap conversion...")
            print("=" * 60)

            table_name = 'Sales.SalesOrderDetail'

            # Check if already a heap table
            if is_heap_table(cursor, table_name):
                print(f"✓ Table {table_name} is already a heap table - skipping conversion")
                return

            # Get current clustered index information
            clustered_info = get_clustered_index_info(cursor, table_name)
            if not clustered_info:
                print(f"✗ No clustered index found on {table_name}")
                return

            clustered_index_name = clustered_info.IndexName
            key_columns = clustered_info.KeyColumns
            is_primary_key = clustered_info.IsPrimaryKey
            constraint_name = clustered_info.ConstraintName

            print(f"Current clustered index: {clustered_index_name}")
            print(f"Key columns: {key_columns}")
            print(f"Is Primary Key: {'Yes' if is_primary_key else 'No'}")
            if constraint_name:
                print(f"Primary Key constraint name: {constraint_name}")

            # Step 1: Create non-clustered index with same key as clustered index
            nc_index_name = f"IX_SalesOrderDetail_NC_{clustered_index_name.replace('PK_', '').replace('_', '')}"

            # Check if NC index already exists
            if index_exists(cursor, nc_index_name, table_name):
                print(f"✓ Non-clustered index {nc_index_name} already exists")
            else:
                try:
                    # Create the non-clustered index
                    create_nc_sql = f"""
                    CREATE NONCLUSTERED INDEX {nc_index_name}
                    ON {table_name} ({key_columns})
                    """

                    print(f"Creating non-clustered index: {nc_index_name}")
                    print(f"SQL: {create_nc_sql}")

                    cursor.execute(create_nc_sql)
                    conn.commit()
                    print(f"✓ Successfully created non-clustered index {nc_index_name}")

                except pyodbc.Error as e:
                    print(f"✗ Failed to create non-clustered index {nc_index_name}: {e}")
                    conn.rollback()
                    return

            # Step 2: Drop the clustered index (handle PRIMARY KEY constraints)
            try:
                if is_primary_key and constraint_name:
                    # For PRIMARY KEY constraints, we need to drop the constraint first
                    drop_constraint_sql = f"ALTER TABLE {table_name} DROP CONSTRAINT {constraint_name}"

                    print(f"Dropping PRIMARY KEY constraint: {constraint_name}")
                    print(f"SQL: {drop_constraint_sql}")

                    cursor.execute(drop_constraint_sql)
                    conn.commit()
                    print(f"✓ Successfully dropped PRIMARY KEY constraint {constraint_name}")
                else:
                    # For regular clustered indexes, drop the index directly
                    drop_clustered_sql = f"DROP INDEX {clustered_index_name} ON {table_name}"

                    print(f"Dropping clustered index: {clustered_index_name}")
                    print(f"SQL: {drop_clustered_sql}")

                    cursor.execute(drop_clustered_sql)
                    conn.commit()
                    print(f"✓ Successfully dropped clustered index {clustered_index_name}")

                print(f"✓ Table {table_name} is now a heap table")

            except pyodbc.Error as e:
                error_msg = f"✗ Failed to drop clustered index/constraint: {e}"
                print(error_msg)
                conn.rollback()
                return

            # Step 3: Recreate PRIMARY KEY as non-clustered constraint
            if is_primary_key and constraint_name:
                try:
                    recreate_pk_sql = f"""
                    ALTER TABLE {table_name}
                    ADD CONSTRAINT PK_SalesOrderDetail
                    PRIMARY KEY NONCLUSTERED ({key_columns})
                    """

                    print("Recreating PRIMARY KEY as non-clustered constraint...")
                    print(f"SQL: {recreate_pk_sql}")

                    cursor.execute(recreate_pk_sql)
                    conn.commit()
                    print("✓ Successfully recreated PRIMARY KEY as non-clustered constraint")

                except pyodbc.Error as e:
                    print(f"✗ Failed to recreate PRIMARY KEY constraint: {e}")
                    conn.rollback()
                    # Continue anyway - heap conversion was successful

            print("\n" + "=" * 60)
            print("Heap table conversion complete!")

            # Display current indexes on Sales.SalesOrderDetail
            print(f"\nCurrent indexes on {table_name}:")
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
                WHERE s.name + '.' + o.name = ?
                AND i.type IN (0, 1, 2)  -- Heap, Clustered, and Non-clustered
                GROUP BY i.name, i.type_desc, i.index_id
                ORDER BY i.index_id
            """, table_name)

            indexes_list = cursor.fetchall()
            for idx in indexes_list:
                index_type = "HEAP" if idx.IndexType == "HEAP" else idx.IndexType
                key_cols = idx.KeyColumns if idx.KeyColumns else "N/A"
                print(f"  - {idx.IndexName or 'HEAP'} ({index_type}): {key_cols}")

    except pyodbc.Error as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
