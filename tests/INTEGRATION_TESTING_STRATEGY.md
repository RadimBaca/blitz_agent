# AdventureWorks2019 Integration Testing Strategy

## Database Structure Analysis Summary

Based on our exploration of the AdventureWorks2019 database, we've identified the optimal tables and strategies for integration testing of the Blitz Agent application.

## Recommended Tables for Integration Testing

After analyzing the database structure, foreign key relationships, and data volumes, we recommend focusing on these **8 core tables**:

### 1. Primary Sales Tables (High Priority)
- **`Sales.SalesOrderHeader`** (31,465 rows)
  - Main sales order table, central to most operations
  - Contains order-level information (dates, totals, customer references)
  - Primary key: `SalesOrderID`

- **`Sales.SalesOrderDetail`** (121,317 rows)
  - Order line items with highest data volume
  - Links orders to products with quantities and pricing
  - Composite primary key: `SalesOrderID, SalesOrderDetailID`

- **`Sales.Customer`** (19,820 rows)
  - Customer master data
  - Links to both Person (individual customers) and Store (business customers)
  - Primary key: `CustomerID`

### 2. Supporting Master Data Tables
- **`Production.Product`** (504 rows)
  - Product catalog referenced by order details
  - Contains pricing, categorization, and product attributes
  - Primary key: `ProductID`

- **`Person.Person`** (19,972 rows)
  - Individual person information for customers
  - Contains names, contact preferences
  - Primary key: `BusinessEntityID`

### 3. Reference/Lookup Tables
- **`Production.ProductSubcategory`** (37 rows)
  - Product categorization hierarchy
  - Links products to categories
  - Primary key: `ProductSubcategoryID`

- **`Sales.SalesTerritory`** (10 rows)
  - Geographic sales regions
  - Referenced by customers and orders
  - Primary key: `TerritoryID`

- **`Person.Address`** (19,614 rows)
  - Address information for billing and shipping
  - Referenced by order headers
  - Primary key: `AddressID`

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

## Integration Testing Scenarios

### 1. OLTP-Style Queries (Frequent, Fast)
- Customer order lookups by ID
- Product information retrieval
- Order detail queries with filtering

### 2. Analytical Queries (Less Frequent, Resource-Intensive)
- Sales performance analysis across territories
- Product popularity and revenue analysis
- Customer behavior patterns

### 3. Complex Join Operations
- Multi-table reports combining orders, customers, products, and geography
- Comprehensive order analysis with all related data
- Time-based trend analysis

### 4. Stress Test Scenarios
- Large result set queries (10,000+ rows)
- Expensive aggregations with grouping
- Non-SARGable queries (to trigger sp_Blitz findings)

## Sample Workload Queries

We've identified 6 key query patterns that represent real-world usage:

1. **Customer Order Lookup** (Weight: 3) - Most frequent
2. **Product Sales Analysis** (Weight: 2) - Analytical
3. **Customer Territory Distribution** (Weight: 1) - Geographic analysis
4. **Comprehensive Order Analysis** (Weight: 1) - Complex joins
5. **Monthly Sales Trends** (Weight: 1) - Time-based aggregation
6. **Product Catalog Scan** (Weight: 2) - Simple table scan

## Testing Tools Created

### 1. `explore_adventureworks.py`
- Database structure analysis
- Table relationship mapping
- Data volume assessment
- Foreign key relationship discovery

### 2. `workload_generator.py`
- Simulates realistic database workload
- Configurable duration (2, 5, or 10 minutes)
- Weighted query execution
- Specific scenario testing for sp_Blitz triggers

## Integration Testing Strategy

### Phase 1: Environment Setup
1. Use the test environment credentials in `tests/.env`
2. Run `explore_adventureworks.py` to verify database connectivity
3. Execute `workload_generator.py` to create initial load

### Phase 2: Blitz Procedure Testing
1. **sp_Blitz Testing**
   - Run after workload generation
   - Focus on general performance recommendations
   - Test filtering by priority levels

2. **sp_BlitzIndex Testing**
   - Generate index-related issues with specific queries
   - Test over-indexing scenarios
   - Validate index recommendation analysis

3. **sp_BlitzCache Testing**
   - Analyze query execution patterns
   - Test expensive query identification
   - Validate cache performance recommendations

### Phase 3: Application Endpoint Testing
Test all Flask endpoints with realistic data:

1. **Initialization endpoints** (`/init/<display_name>`)
2. **Data filtering and display** (`/<display_name>`)
3. **Analysis endpoints** (`/analyze/<display_name>/<rec_id>`)
4. **Recommendation management** (`/recommendations`)

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

## Expected sp_Blitz Findings

Based on the workload patterns, expect to see:

- **Index recommendations** from sp_BlitzIndex
- **Query performance issues** from sp_BlitzCache
- **Over-indexing warnings** for heavily queried tables
- **Missing index suggestions** for complex analytical queries
- **Cache plan optimization** recommendations

## Next Steps

1. Run the workload generator to create database activity
2. Execute sp_Blitz procedures through the application
3. Develop automated integration tests based on these scenarios
4. Create test data assertions for expected recommendations
5. Implement end-to-end testing workflows

This strategy provides comprehensive coverage of the application's functionality while using realistic, interconnected data that represents typical OLTP and analytical workloads.
