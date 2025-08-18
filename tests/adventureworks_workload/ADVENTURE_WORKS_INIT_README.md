# AdventureWorks Over-Indexing Simulation

## Key Relationships for Testing

The selected tables form a well-connected relationship graph:

```
Sales.SalesOrderHeader
├── Sales.Customer (CustomerID)
│   ├── Person.Person (PersonID)
│   └── Sales.SalesTerritory (TerritoryID)
├── Person.Address (BillToAddressID, ShipToAddressID)
└── Sales.SalesOrderDetail (SalesOrderID)
    └── Production.Product (ProductID)
        └── Production.ProductSubcategory (ProductSubcategoryID)
```

## Data Characteristics

| Table | Rows | Key Features |
|-------|------|--------------|
| SalesOrderDetail | 121,317 | Highest volume, frequent joins |
| SalesOrderHeader | 31,465 | Core transactional data |
| Person.Person | 19,972 | Customer demographics |
| Customer | 19,820 | Customer master |
| Address | 19,614 | Geographic data |
| Product | 504 | Product catalog |
| ProductSubcategory | 37 | Lookup table |
| SalesTerritory | 10 | Small reference table |

## Overview
The `init_adventure_works.py` script creates a realistic over-indexing scenario on the `Production.Product` table in AdventureWorks database for testing index optimization tools.

## Purpose
This script simulates common over-indexing problems found in production databases:
1. **Unused indexes** - Indexes that are never accessed by queries
2. **Redundant indexes** - Indexes that duplicate functionality of other indexes
3. **Covering indexes** - Potentially over-engineered indexes with many included columns
4. **Compound indexes** - Multi-column indexes that may be beneficial but could cause maintenance overhead

## Existing Indexes on Production.Product
- `PK_Product_ProductID` (ProductID) - Primary Key
- `AK_Product_Name` (Name) - Unique constraint
- `AK_Product_ProductNumber` (ProductNumber) - Unique constraint
- `AK_Product_rowguid` (rowguid) - Unique constraint

## New Indexes Created

### Set 1: Primary Over-Indexing Patterns

### 1. IX_Product_DaysToManufacture
- **Type**: Never used index
- **Columns**: DaysToManufacture
- **Problem**: This index will never be used because DaysToManufacture is not referenced in WHERE, ORDER BY, or JOIN clauses in the workload

### 2. IX_Product_SubcategoryID_ListPrice_FinishedGoods
- **Type**: Compound index (beneficial)
- **Columns**: ProductSubcategoryID, ListPrice, FinishedGoodsFlag
- **Purpose**: Supports the common query pattern: `WHERE ProductSubcategoryID = X AND ListPrice BETWEEN Y AND Z AND FinishedGoodsFlag = 1`

### 3. IX_Product_ListPrice_Included
- **Type**: Covering index
- **Key Columns**: ListPrice
- **Included Columns**: ProductID, Name, ProductNumber, Color, FinishedGoodsFlag, ProductSubcategoryID
- **Purpose**: Covers product search queries by price with all commonly selected columns

### 4. IX_Product_FinishedGoodsFlag
- **Type**: Redundant index
- **Columns**: FinishedGoodsFlag
- **Problem**: Made redundant by compound index #2, which has FinishedGoodsFlag as the third column

### Set 2: Additional Over-Indexing Scenarios

### 5. IX_Product_SellStartDate
- **Type**: Never used index
- **Columns**: SellStartDate
- **Problem**: This index will never be used because SellStartDate is not referenced in the workload queries

### 6. IX_Product_Color_SafetyStock_ReorderPoint
- **Type**: Compound index (for inventory management)
- **Columns**: Color, SafetyStockLevel, ReorderPoint
- **Purpose**: Supports inventory queries filtering by color and safety stock levels

### 7. IX_Product_Color_Included
- **Type**: Covering index
- **Key Columns**: Color
- **Included Columns**: ProductID, Name, ProductNumber, ListPrice, SafetyStockLevel, ReorderPoint
- **Purpose**: Covers product searches by color with inventory-related columns included

### 8. IX_Product_SafetyStockLevel
- **Type**: Redundant index
- **Columns**: SafetyStockLevel
- **Problem**: Made redundant by compound index #6, which has SafetyStockLevel as the second column## Setup

### Prerequisites
1. AdventureWorks2019 database
2. Connection to SQL Server with CREATE INDEX permissions
3. Python environment with required packages (pyodbc, python-dotenv)

### Environment Configuration
1. Copy `.env.example` to `.env` in the tests directory
2. Update the connection parameters:
   ```
   MOCK_MSSQL_HOST=your_server
   MOCK_MSSQL_PORT=1433
   MOCK_MSSQL_DB=AdventureWorks2019
   MOCK_MSSQL_USER=your_username
   MOCK_MSSQL_PASSWORD=your_password
   ```

## Usage

### Create Over-Indexing Scenario
```bash
# Activate virtual environment
source venv/bin/activate

# Create the indexes (default action)
python tests/init_adventure_works.py

# Or explicitly specify init
python tests/init_adventure_works.py init
```

### Verify Index Usage
```bash
# Show index usage statistics
python tests/init_adventure_works.py verify
```

### Clean Up
```bash
# Remove all created indexes
python tests/init_adventure_works.py cleanup
```

## Testing Workflow

1. **Initialize**: Run `init_adventure_works.py` to create the over-indexing scenario
2. **Generate Workload**: Run `workload_generator.py` to generate realistic database activity
3. **Analyze**: Run your index optimization tools to detect the over-indexing issues
4. **Verify**: Use `verify` command to see which indexes are actually being used
5. **Cleanup**: Run `cleanup` command to remove test indexes

## Expected Optimization Recommendations

A good index optimization tool should detect:

### Unused Indexes:
- `IX_Product_DaysToManufacture` - Never used because DaysToManufacture isn't queried
- `IX_Product_SellStartDate` - Never used because SellStartDate isn't queried

### Redundant Indexes:
- `IX_Product_FinishedGoodsFlag` - Redundant due to compound index #2
- `IX_Product_SafetyStockLevel` - Redundant due to compound index #6

### Potentially Over-Engineered:
- Both covering indexes might be flagged if their included columns aren't frequently used together
- Compound indexes might be questioned if only partial key usage is detected

### Performance Impact:
- 8 total indexes create significant maintenance overhead for INSERT/UPDATE/DELETE operations
- Storage overhead from duplicated data in covering indexes
- Query plan confusion from multiple similar indexes

## Integration with Existing Tests

This script is designed to work alongside:
- `workload_generator.py` - Generates realistic query workload
- `test_over_indexing_dao.py` - Tests over-indexing detection functionality
- Other integration tests in the test suite

## Safety Features

- **Existence Check**: Only creates indexes if they don't already exist
- **Transaction Safety**: Uses database transactions with rollback on errors
- **Clean Shutdown**: Graceful error handling and cleanup procedures
