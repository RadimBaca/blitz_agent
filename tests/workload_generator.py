#!/usr/bin/env python3
"""
AdventureWorks2019 Integration Test Workload Generator
This script creates a realistic workload on the suggested tables for integration testing.
"""

import os
import pyodbc
import time
import random
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timedelta

# Load environment variables from the tests directory
test_dir = Path(__file__).parent
load_dotenv(test_dir / '.env')

# Global list to track inserted records for cleanup
inserted_records = {
    'customers': [],
    'addresses': [],
    'products': [],
    'orders': [],
    'order_details': []
}

def get_connection():
    """Create database co        # Clean up any inserted records
        cleanup_inserted_records()

    except pyodbc.Error as e:
        print(f"Database error: {e}")
        # Try to clean up even if there was an error
        try:
            cleanup_inserted_records()
        except Exception:
            pass
    except KeyboardInterrupt:
        print("\nProgram interrupted by user")
        # Try to clean up even if interrupted
        try:
            cleanup_inserted_records()
        except Exception:
            passst environment variables"""
    connection_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('MOCK_MSSQL_HOST')},{os.getenv('MOCK_MSSQL_PORT')};"
        f"DATABASE={os.getenv('MOCK_MSSQL_DB')};"
        f"UID={os.getenv('MOCK_MSSQL_USER')};"
        f"PWD={os.getenv('MOCK_MSSQL_PASSWORD')};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(connection_string)

def get_sample_data():
    """Get sample data for generating realistic queries and DML operations"""
    with get_connection() as conn:
        cursor = conn.cursor()

        # Get sample customer IDs
        cursor.execute("SELECT TOP 100 CustomerID FROM Sales.Customer ORDER BY NEWID()")
        customer_ids = [row[0] for row in cursor.fetchall()]

        # Get sample product IDs
        cursor.execute("SELECT TOP 50 ProductID FROM Production.Product WHERE FinishedGoodsFlag = 1 ORDER BY NEWID()")
        product_ids = [row[0] for row in cursor.fetchall()]

        # Get sample territory IDs
        cursor.execute("SELECT TerritoryID FROM Sales.SalesTerritory")
        territory_ids = [row[0] for row in cursor.fetchall()]

        # Get sample subcategory IDs
        cursor.execute("SELECT ProductSubcategoryID FROM Production.ProductSubcategory")
        subcategory_ids = [row[0] for row in cursor.fetchall()]

        # Get sample person IDs (for customers)
        cursor.execute("SELECT TOP 100 BusinessEntityID FROM Person.Person WHERE PersonType = 'IN' ORDER BY NEWID()")
        person_ids = [row[0] for row in cursor.fetchall()]

        return {
            'customer_ids': customer_ids,
            'product_ids': product_ids,
            'territory_ids': territory_ids,
            'subcategory_ids': subcategory_ids,
            'person_ids': person_ids
        }

def run_workload_queries(sample_data):
    """Execute a variety of OLTP-style queries with different predicates"""

    queries = [
        # OLTP Query 1: Customer lookup by various attributes
        {
            'name': 'Customer Lookup by Territory',
            'query': f'''
            SELECT TOP 20
                c.CustomerID,
                c.AccountNumber,
                p.FirstName,
                p.LastName,
                st.Name AS Territory
            FROM Sales.Customer c
            LEFT JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
            JOIN Sales.SalesTerritory st ON c.TerritoryID = st.TerritoryID
            WHERE c.TerritoryID = {random.choice(sample_data['territory_ids'])}
            ORDER BY c.CustomerID
            ''',
            'weight': 4
        },

        # OLTP Query 2: Product search by category and price range
        {
            'name': 'Product Search by Category',
            'query': f'''
            SELECT
                pr.ProductID,
                pr.Name,
                pr.ProductNumber,
                pr.ListPrice,
                pr.Color,
                psc.Name AS Subcategory
            FROM Production.Product pr
            LEFT JOIN Production.ProductSubcategory psc ON pr.ProductSubcategoryID = psc.ProductSubcategoryID
            WHERE pr.ProductSubcategoryID = {random.choice(sample_data['subcategory_ids'])}
            AND pr.ListPrice BETWEEN {random.uniform(10, 500):.2f} AND {random.uniform(500, 2000):.2f}
            AND pr.FinishedGoodsFlag = 1
            ORDER BY pr.ListPrice DESC
            ''',
            'weight': 3
        },

        # OLTP Query 3: Order lookup by customer with status filter
        {
            'name': 'Customer Order History',
            'query': f'''
            SELECT TOP 30
                soh.SalesOrderID,
                soh.OrderDate,
                soh.Status,
                soh.TotalDue,
                COUNT(sod.SalesOrderDetailID) AS ItemCount
            FROM Sales.SalesOrderHeader soh
            LEFT JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
            WHERE soh.CustomerID = {random.choice(sample_data['customer_ids'])}
            AND soh.Status IN (1, 2, 5)
            GROUP BY soh.SalesOrderID, soh.OrderDate, soh.Status, soh.TotalDue
            ORDER BY soh.OrderDate DESC
            ''',
            'weight': 4
        },

        # OLTP Query 4: Product inventory check
        {
            'name': 'Product Availability Check',
            'query': f'''
            SELECT
                pr.ProductID,
                pr.Name,
                pr.ListPrice,
                pr.SafetyStockLevel,
                pr.ReorderPoint,
                CASE
                    WHEN pr.FinishedGoodsFlag = 1 THEN 'Available'
                    ELSE 'Not Available'
                END AS Availability
            FROM Production.Product pr
            WHERE pr.ProductID IN ({','.join(map(str, random.sample(sample_data['product_ids'], min(10, len(sample_data['product_ids'])))))})
            ORDER BY pr.Name
            ''',
            'weight': 3
        },

        # OLTP Query 5: Address lookup for shipping
        {
            'name': 'Address Lookup by City',
            'query': '''
            SELECT TOP 25
                a.AddressID,
                a.AddressLine1,
                a.AddressLine2,
                a.City,
                a.PostalCode,
                sp.Name AS StateProvince
            FROM Person.Address a
            JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
            WHERE a.City IN ('Seattle', 'Redmond', 'Bellevue', 'Kirkland', 'Bothell')
            ORDER BY a.City, a.AddressLine1
            ''',
            'weight': 2
        },

        # OLTP Query 6: Order details with product info
        {
            'name': 'Order Line Items Detail',
            'query': f'''
            SELECT
                sod.SalesOrderDetailID,
                sod.OrderQty,
                sod.UnitPrice,
                sod.LineTotal,
                pr.Name AS ProductName,
                pr.ProductNumber
            FROM Sales.SalesOrderDetail sod
            JOIN Production.Product pr ON sod.ProductID = pr.ProductID
            WHERE sod.ProductID = {random.choice(sample_data['product_ids'])}
            AND sod.OrderQty >= {random.randint(1, 5)}
            ORDER BY sod.LineTotal DESC
            ''',
            'weight': 3
        },

        # OLTP Query 7: Customer search by name pattern
        {
            'name': 'Customer Name Search',
            'query': f'''
            SELECT TOP 20
                c.CustomerID,
                p.FirstName,
                p.LastName,
                p.EmailPromotion,
                c.AccountNumber
            FROM Sales.Customer c
            JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
            WHERE p.LastName LIKE '{random.choice(['A', 'B', 'C', 'D', 'E', 'F', 'G'])}%'
            AND p.EmailPromotion > 0
            ORDER BY p.LastName, p.FirstName
            ''',
            'weight': 2
        },

        # OLTP Query 8: Recent high-value orders
        {
            'name': 'High Value Recent Orders',
            'query': f'''
            SELECT TOP 15
                soh.SalesOrderID,
                soh.OrderDate,
                soh.TotalDue,
                c.CustomerID,
                st.Name AS Territory
            FROM Sales.SalesOrderHeader soh
            JOIN Sales.Customer c ON soh.CustomerID = c.CustomerID
            JOIN Sales.SalesTerritory st ON c.TerritoryID = st.TerritoryID
            WHERE soh.TotalDue > {random.uniform(1000, 5000):.2f}
            AND soh.OrderDate >= DATEADD(month, -{random.randint(1, 6)}, GETDATE())
            ORDER BY soh.TotalDue DESC
            ''',
            'weight': 2
        }
    ]

    return queries

def run_dml_operations(_sample_data):
    """Execute DML operations (INSERT, UPDATE, DELETE) and track changes"""

    operations = []

    # DML Operation 1: Insert new customer (Person + Customer)
    operations.append({
        'name': 'Insert New Customer',
        'operation': 'insert_customer',
        'weight': 1
    })

    # DML Operation 2: Insert new address
    operations.append({
        'name': 'Insert New Address',
        'operation': 'insert_address',
        'weight': 1
    })

    # DML Operation 3: Update customer territory
    operations.append({
        'name': 'Update Customer Territory',
        'operation': 'update_customer',
        'weight': 2
    })

    # DML Operation 4: Update product price
    operations.append({
        'name': 'Update Product Price',
        'operation': 'update_product',
        'weight': 2
    })

    # DML Operation 5: Insert sales order with details
    operations.append({
        'name': 'Insert Sales Order',
        'operation': 'insert_order',
        'weight': 1
    })

    return operations

def execute_dml_operation(operation, sample_data):
    """Execute a specific DML operation"""

    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            if operation == 'insert_customer':
                # Insert a new person first
                first_names = ['John', 'Jane', 'Mike', 'Sarah', 'David', 'Lisa', 'Mark', 'Emma']
                last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']

                # Get next BusinessEntityID
                cursor.execute("SELECT MAX(BusinessEntityID) + 1 FROM Person.BusinessEntity")
                next_id = cursor.fetchone()[0] or 1

                # Insert BusinessEntity first
                cursor.execute("""
                    INSERT INTO Person.BusinessEntity (BusinessEntityID, rowguid, ModifiedDate)
                    VALUES (?, NEWID(), GETDATE())
                """, next_id)

                # Insert Person
                first_name = random.choice(first_names)
                last_name = random.choice(last_names)
                cursor.execute("""
                    INSERT INTO Person.Person (BusinessEntityID, PersonType, NameStyle, FirstName, LastName, EmailPromotion, rowguid, ModifiedDate)
                    VALUES (?, 'IN', 0, ?, ?, ?, NEWID(), GETDATE())
                """, next_id, first_name, last_name, random.randint(0, 2))

                # Insert Customer
                territory_id = random.choice(sample_data['territory_ids'])
                cursor.execute("""
                    INSERT INTO Sales.Customer (PersonID, TerritoryID, AccountNumber, rowguid, ModifiedDate)
                    VALUES (?, ?, 'AW' + RIGHT('00000000' + CAST(? AS VARCHAR), 8), NEWID(), GETDATE())
                """, next_id, territory_id, next_id)

                # Get the new CustomerID
                cursor.execute("SELECT CustomerID FROM Sales.Customer WHERE PersonID = ?", next_id)
                customer_id = cursor.fetchone()[0]

                inserted_records['customers'].append({'person_id': next_id, 'customer_id': customer_id})
                conn.commit()
                return f"Inserted customer: {first_name} {last_name} (CustomerID: {customer_id})"

            elif operation == 'insert_address':
                # Insert a new address
                cities = ['Test City', 'Sample Town', 'Demo Village', 'Mock Borough']
                streets = ['123 Test St', '456 Sample Ave', '789 Demo Blvd', '321 Mock Lane']

                # Get a random StateProvinceID
                cursor.execute("SELECT TOP 1 StateProvinceID FROM Person.StateProvince ORDER BY NEWID()")
                state_id = cursor.fetchone()[0]

                street = random.choice(streets)
                city = random.choice(cities)
                postal_code = f"{random.randint(10000, 99999)}"

                cursor.execute("""
                    INSERT INTO Person.Address (AddressLine1, City, StateProvinceID, PostalCode, rowguid, ModifiedDate)
                    VALUES (?, ?, ?, ?, NEWID(), GETDATE())
                """, street, city, state_id, postal_code)

                # Get the new AddressID
                cursor.execute("SELECT @@IDENTITY")
                address_id = cursor.fetchone()[0]

                inserted_records['addresses'].append(address_id)
                conn.commit()
                return f"Inserted address: {street}, {city} (AddressID: {address_id})"

            elif operation == 'update_customer':
                # Update customer territory
                if sample_data['customer_ids']:
                    customer_id = random.choice(sample_data['customer_ids'])
                    new_territory = random.choice(sample_data['territory_ids'])

                    cursor.execute("""
                        UPDATE Sales.Customer
                        SET TerritoryID = ?, ModifiedDate = GETDATE()
                        WHERE CustomerID = ?
                    """, new_territory, customer_id)

                    conn.commit()
                    return f"Updated customer {customer_id} territory to {new_territory}"

            elif operation == 'update_product':
                # Update product price (only for test products or increase existing by small amount)
                if sample_data['product_ids']:
                    product_id = random.choice(sample_data['product_ids'])
                    price_change = random.uniform(0.95, 1.05)  # Small price adjustment

                    cursor.execute("""
                        UPDATE Production.Product
                        SET ListPrice = ListPrice * ?, ModifiedDate = GETDATE()
                        WHERE ProductID = ? AND ListPrice > 0
                    """, price_change, product_id)

                    conn.commit()
                    return f"Updated product {product_id} price by factor {price_change:.3f}"

            elif operation == 'insert_order':
                # Insert a sales order with details (simplified)
                if sample_data['customer_ids'] and sample_data['product_ids']:
                    customer_id = random.choice(sample_data['customer_ids'])

                    # Get customer's territory and a billing address
                    cursor.execute("""
                        SELECT c.TerritoryID, a.AddressID
                        FROM Sales.Customer c
                        JOIN Sales.SalesOrderHeader soh ON c.CustomerID = soh.CustomerID
                        JOIN Person.Address a ON soh.BillToAddressID = a.AddressID
                        WHERE c.CustomerID = ?
                        GROUP BY c.TerritoryID, a.AddressID
                    """, customer_id)

                    result = cursor.fetchone()
                    if result:
                        territory_id, address_id = result

                        # Insert SalesOrderHeader
                        cursor.execute("""
                            INSERT INTO Sales.SalesOrderHeader
                            (RevisionNumber, OrderDate, DueDate, Status, OnlineOrderFlag,
                             CustomerID, TerritoryID, BillToAddressID, ShipToAddressID,
                             ShipMethodID, SubTotal, TaxAmt, Freight, TotalDue, rowguid, ModifiedDate)
                            VALUES (0, GETDATE(), DATEADD(day, 7, GETDATE()), 1, 1,
                                   ?, ?, ?, ?, 1, 100.00, 8.00, 5.00, 113.00, NEWID(), GETDATE())
                        """, customer_id, territory_id, address_id, address_id)

                        # Get the new SalesOrderID
                        cursor.execute("SELECT @@IDENTITY")
                        order_id = cursor.fetchone()[0]

                        inserted_records['orders'].append(order_id)
                        conn.commit()
                        return f"Inserted sales order {order_id} for customer {customer_id}"

    except pyodbc.Error as e:
        return f"DML Error: {str(e)[:100]}..."

    return "DML operation completed"

def cleanup_inserted_records():
    """Clean up all inserted records in reverse order"""
    print("\nCleaning up inserted records...")

    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            # Delete in reverse order of dependencies

            # Delete order details (if any were inserted)
            for order_detail_id in inserted_records['order_details']:
                try:
                    cursor.execute("DELETE FROM Sales.SalesOrderDetail WHERE SalesOrderDetailID = ?", order_detail_id)
                except pyodbc.Error:
                    pass  # Continue cleanup even if some deletes fail

            # Delete sales orders
            for order_id in inserted_records['orders']:
                try:
                    cursor.execute("DELETE FROM Sales.SalesOrderHeader WHERE SalesOrderID = ?", order_id)
                except pyodbc.Error:
                    pass

            # Delete customers
            for customer_record in inserted_records['customers']:
                try:
                    cursor.execute("DELETE FROM Sales.Customer WHERE CustomerID = ?", customer_record['customer_id'])
                    cursor.execute("DELETE FROM Person.Person WHERE BusinessEntityID = ?", customer_record['person_id'])
                    cursor.execute("DELETE FROM Person.BusinessEntity WHERE BusinessEntityID = ?", customer_record['person_id'])
                except pyodbc.Error:
                    pass

            # Delete addresses
            for address_id in inserted_records['addresses']:
                try:
                    cursor.execute("DELETE FROM Person.Address WHERE AddressID = ?", address_id)
                except pyodbc.Error:
                    pass

            conn.commit()

            total_cleaned = (len(inserted_records['customers']) +
                           len(inserted_records['addresses']) +
                           len(inserted_records['orders']) +
                           len(inserted_records['order_details']))

            print(f"Cleaned up {total_cleaned} records")

            # Reset tracking
            for key in inserted_records:
                inserted_records[key].clear()

    except pyodbc.Error as e:
        print(f"Cleanup error: {e}")

def execute_workload(duration_minutes=5, _concurrent_sessions=1):
    """
    Execute workload for specified duration with DML operations

    Args:
        duration_minutes: How long to run the workload
        _concurrent_sessions: Number of concurrent database sessions (for future enhancement)
    """
    print(f"Starting workload for {duration_minutes} minutes...")

    # Get sample data for queries
    sample_data = get_sample_data()

    # Get queries and DML operations
    queries = run_workload_queries(sample_data)
    dml_operations = run_dml_operations(sample_data)

    # Create weighted operation list (favor SELECT over DML)
    weighted_operations = []

    # Add queries (more frequent)
    for query in queries:
        for _ in range(query['weight']):
            weighted_operations.append(('query', query))

    # Add DML operations (less frequent)
    for operation in dml_operations:
        for _ in range(operation['weight']):
            weighted_operations.append(('dml', operation))

    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)

    operation_count = 0
    query_count = 0
    dml_count = 0
    total_execution_time = 0

    try:
        while time.time() < end_time:
            # Randomly select an operation
            operation_type, operation = random.choice(weighted_operations)

            try:
                operation_start = time.time()

                if operation_type == 'query':
                    with get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute(operation['query'])
                        rows = cursor.fetchall()
                        result_text = f"{operation['name']} - {len(rows)} rows"
                        query_count += 1
                else:  # DML operation
                    result_text = execute_dml_operation(operation['operation'], sample_data)
                    dml_count += 1

                operation_end = time.time()
                execution_time = operation_end - operation_start
                total_execution_time += execution_time
                operation_count += 1

                print(f"Operation {operation_count}: {result_text} in {execution_time:.2f}s")

            except pyodbc.Error as e:
                print(f"Error executing {operation.get('name', operation.get('operation'))}: {str(e)[:100]}...")

            # Random delay between operations (0.5-3 seconds)
            time.sleep(random.uniform(0.5, 3.0))

    except KeyboardInterrupt:
        print("\nWorkload interrupted by user")

    elapsed_time = time.time() - start_time
    avg_execution_time = total_execution_time / operation_count if operation_count > 0 else 0

    print("\n=== WORKLOAD SUMMARY ===")
    print(f"Total runtime: {elapsed_time:.1f} seconds")
    print(f"Total operations: {operation_count} (Queries: {query_count}, DML: {dml_count})")
    print(f"Average operation time: {avg_execution_time:.2f} seconds")
    print(f"Operations per minute: {operation_count / (elapsed_time / 60):.1f}")

    return True  # Indicate workload completed

def test_specific_scenarios():
    """Test specific database scenarios that might trigger sp_Blitz* findings"""

    print("\n=== TESTING SPECIFIC SCENARIOS ===")

    scenarios = [
        {
            'name': 'Large Result Set Query',
            'query': '''
            SELECT
                soh.*,
                sod.*,
                pr.Name,
                pr.ListPrice
            FROM Sales.SalesOrderHeader soh
            JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
            JOIN Production.Product pr ON sod.ProductID = pr.ProductID
            WHERE soh.OrderDate >= DATEADD(year, -1, GETDATE())
            '''
        },
        {
            'name': 'Expensive Aggregation',
            'query': '''
            SELECT
                c.CustomerID,
                COUNT(*) AS OrderCount,
                SUM(soh.TotalDue) AS TotalSpent,
                AVG(soh.TotalDue) AS AvgOrderValue,
                MAX(soh.OrderDate) AS LastOrderDate,
                STRING_AGG(CAST(soh.SalesOrderID AS VARCHAR), ',') AS OrderList
            FROM Sales.Customer c
            JOIN Sales.SalesOrderHeader soh ON c.CustomerID = soh.CustomerID
            GROUP BY c.CustomerID
            HAVING COUNT(*) > 5
            ORDER BY TotalSpent DESC
            '''
        },
        {
            'name': 'Non-SARGable Query',
            'query': '''
            SELECT
                ProductID,
                Name,
                ListPrice
            FROM Production.Product
            WHERE SUBSTRING(Name, 1, 3) = 'Mou'
            OR YEAR(ModifiedDate) = 2008
            '''
        }
    ]

    for scenario in scenarios:
        print(f"\nExecuting: {scenario['name']}")
        try:
            with get_connection() as conn:
                cursor = conn.cursor()
                start_time = time.time()
                cursor.execute(scenario['query'])
                rows = cursor.fetchall()
                execution_time = time.time() - start_time
                print(f"  Result: {len(rows)} rows in {execution_time:.2f} seconds")
        except pyodbc.Error as e:
            print(f"  Error: {e}")

def main():
    """Main function"""
    print("AdventureWorks2019 Integration Test Workload Generator")
    print("=" * 55)

    try:
        # Test connection
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION, DB_NAME()")
            version, db_name = cursor.fetchone()
            print(f"Connected to database: {db_name}")
            print(f"SQL Server version: {version[:100]}...")

        print("\nOptions:")
        print("1. Run light workload (2 minutes)")
        print("2. Run medium workload (5 minutes)")
        print("3. Run heavy workload (10 minutes)")
        print("4. Test specific scenarios only")
        print("5. Exit")

        choice = input("\nSelect option (1-5): ").strip()

        workload_completed = False

        if choice == '1':
            workload_completed = execute_workload(duration_minutes=2)
        elif choice == '2':
            workload_completed = execute_workload(duration_minutes=5)
        elif choice == '3':
            workload_completed = execute_workload(duration_minutes=10)
        elif choice == '4':
            test_specific_scenarios()
        elif choice == '5':
            print("Exiting...")
            return
        else:
            print("Invalid choice. Running light workload...")
            workload_completed = execute_workload(duration_minutes=2)

        # Always run specific scenarios after workload (if workload was executed)
        if workload_completed and choice in ['1', '2', '3']:
            test_specific_scenarios()

        # Clean up any inserted records
        cleanup_inserted_records()

    except pyodbc.Error as e:
        print(f"Database error: {e}")
        # Try to clean up even if there was an error
        try:
            cleanup_inserted_records()
        except:
            pass
    except KeyboardInterrupt:
        print("\nProgram interrupted by user")
        # Try to clean up even if interrupted
        try:
            cleanup_inserted_records()
        except:
            pass

if __name__ == "__main__":
    main()
