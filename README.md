# Chinook Music Store: Azure SQL to Snowflake Data Warehouse Pipeline

End-to-end ETL pipeline using **Azure Data Factory** to migrate Chinook music store data from **Azure SQL Database** → **Azure Blob Storage** (Parquet) → **Snowflake Data Warehouse** with dimensional modeling.

---

## 🎯 Project Overview

**What it does:**
- **Extracts** music store data from Azure SQL Database
- **Stages** data as Parquet files in Azure Blob Storage
- **Loads** raw data into Snowflake staging tables with audit columns
- **Transforms** data into a star schema data warehouse
- Implements incremental loading and maintains referential integrity

**Business Use Case:** Analytics platform for music store to analyze sales trends, customer behavior, and product performance.

---

## 🏗️ Architecture

```
Azure SQL Database (Source)
         ↓
   [Extract Pipeline]
         ↓
Azure Blob Storage (Parquet Files)
         ↓
   [Transform & Load Pipeline]
         ↓
Snowflake Data Warehouse
    • STAGE Schema (Landing)
    • DW Schema (Star Model)
```

**Complete Data Flow:**
1. **Extract:** Azure SQL → Parquet files in Blob Storage
2. **Stage:** Parquet → Snowflake STAGE tables (with audit columns)
3. **Transform:** STAGE → Snowflake DW dimensions
4. **Load:** Dimensions → Snowflake SALES_FACT

---

## 📊 Data Model

**Source: Azure SQL Database**
- Chinook sample database with tables: Album, Artist, Customer, Invoice, InvoiceLine, etc.

**Intermediate: Azure Blob Storage**
- Parquet files: `Album.parquet`, `Artist.parquet`, `Customer.parquet`, `Invoice.parquet`, `InvoiceLine.parquet`

**Staging Layer (Snowflake STAGE Schema)**
- Album, Artist, Customer, Invoice, InvoiceLine tables
- Each with audit columns: `Created_By`, `Created_Dt`

**Data Warehouse (Snowflake DW Schema) - Star Schema**

**Dimensions:**
- `DATE_DIM` - Calendar dimension (30 years)
- `TIME_DIM` - Time dimension (1,440 minutes/day)
- `CUSTOMER_DIM` - Customer details with SCD Type 2
- `ARTIST_DIM` - Artist information

**Fact:**
- `SALES_FACT` - Sales transactions with foreign keys to all dimensions

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| Source Database | Azure SQL Database |
| Staging Storage | Azure Blob Storage |
| ETL Orchestration | Azure Data Factory |
| Target Warehouse | Snowflake |
| File Format | Parquet |
| Security | Azure Key Vault |

---

## 📁 Project Structure

```
├── pipeline/
│   ├── extract_SQLDB_PL          # Extract from Azure SQL to Parquet
│   ├── stage_Partquet_PL         # Load Parquet to Snowflake STAGE
│   ├── load_CUSTOMER_DIM_PL      # Load customer dimension
│   ├── load_ARTIST_DIM_PL        # Load artist dimension
│   ├── load_ALBUM_DIM_PL         # Load album dimension
│   ├── load_INVOICE_DIM_PL       # Load invoice dimension
│   └── load_SALES_FACT_PL        # Load sales fact
├── dataflow/
│   ├── transform_source_parquet  # Parquet transformations
│   └── df_load_*_DIM             # Dimension data flows
├── dataset/
│   ├── chinook_ds                # Azure SQL dataset
│   ├── chinook_ds_parquet        # Blob Storage dataset
│   └── chinook_ds_snowflake      # Snowflake dataset
├── linkedService/
│   ├── ls_azuresqldb             # Azure SQL connection
│   ├── ls_azureblobstorage       # Blob Storage connection
│   └── ls_snowflake              # Snowflake connection
└── sql/
    ├── snowflake_setup.sql       # Snowflake schema creation
    └── fact_load.sql             # Fact table load query
```

---

## 🚀 Setup Instructions

### 1. Azure SQL Database Setup
```sql
-- Chinook database should already be loaded
-- Verify tables exist:
SELECT * FROM Album;
SELECT * FROM Artist;
SELECT * FROM Customer;
SELECT * FROM Invoice;
SELECT * FROM InvoiceLine;
```

### 2. Azure Blob Storage Setup
- Create storage account
- Create container: `stagedata`
- Note connection string for ADF

### 3. Snowflake Setup
```sql
CREATE WAREHOUSE CHINOOK_WH WAREHOUSE_SIZE='XSMALL';
CREATE DATABASE CHINOOK_DB;
CREATE SCHEMA CHINOOK_DB.STAGE;
CREATE SCHEMA CHINOOK_DB.DW;
CREATE ROLE CHINOOK_ROLE;
CREATE USER CHINOOK_USER PASSWORD='SecurePassword' 
    DEFAULT_ROLE=CHINOOK_ROLE DEFAULT_WAREHOUSE=CHINOOK_WH;

-- Grant permissions
GRANT ROLE CHINOOK_ROLE TO USER CHINOOK_USER;
GRANT USAGE ON WAREHOUSE CHINOOK_WH TO ROLE CHINOOK_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA CHINOOK_DB.STAGE TO ROLE CHINOOK_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA CHINOOK_DB.DW TO ROLE CHINOOK_ROLE;
```

### 4. Azure Data Factory Setup
1. Create ADF instance
2. Store credentials in Azure Key Vault:
   - Azure SQL connection string
   - Snowflake password
3. Import JSON files from this repository
4. Configure linked services:
   - `ls_azuresqldb` → Azure SQL Database
   - `ls_azureblobstorage` → Blob Storage
   - `ls_snowflake` → Snowflake

### 5. Run Pipelines (in order)
1. **`extract_SQLDB_PL`** → Extract Azure SQL to Parquet files
2. **`stage_Partquet_PL`** → Load Parquet to Snowflake STAGE
3. **`load_CUSTOMER_DIM_PL`** → Load customer dimension
4. **`load_ARTIST_DIM_PL`** → Load artist dimension
5. **`load_ALBUM_DIM_PL`** → Load album dimension
6. **`load_INVOICE_DIM_PL`** → Load invoice dimension
7. **`load_INVOICELINE_DIM_PL`** → Load invoice line dimension
8. **`load_SALES_FACT_PL`** → Load sales fact table

---

## 🔄 Pipeline Details

### Pipeline 1: Extract from Azure SQL (`extract_SQLDB_PL`)
**Purpose:** Export data from Azure SQL Database to Parquet files

**Features:**
- ForEach loop iterates over table list
- Copy Activity exports to Parquet format
- Compression: Snappy
- Destination: Azure Blob Storage (`stagedata` container)

**Output:** `Album.parquet`, `Artist.parquet`, `Customer.parquet`, etc.

---

### Pipeline 2: Stage Parquet (`stage_Partquet_PL`)
**Purpose:** Load Parquet files into Snowflake staging tables

**Features:**
- ForEach loop processes each Parquet file
- Data Flow adds audit columns:
  - `Created_By = 'ADF_PIPELINE'`
  - `Created_Dt = CURRENT_DATE()`
- Select transformation converts column names to uppercase (Parquet mixed case → Snowflake UPPERCASE)
- Dynamic schema handling with schema drift

---

### Pipeline 3-7: Dimension Loads
**Purpose:** Transform STAGE data into dimensional model

**Features:**
- Generate surrogate keys using Snowflake sequences
- Add metadata columns (SOURCE_ID, DATE_TO_WAREHOUSE)
- Implement SCD Type 2 for CUSTOMER_DIM (track history)
- Incremental loading to avoid duplicates

---

### Pipeline 8: Sales Fact Load (`load_SALES_FACT_PL`)
**Purpose:** Create fact table with aggregated sales data

**Query Logic:**
```sql
WITH aggsales AS (
    SELECT 
        cd.CUSTOMER_KEY,
        i."InvoiceId" AS INVOICE_ID,
        i."InvoiceDate" AS SALE_DATE,
        SUM(il."Quantity" * il."UnitPrice") AS TOTAL_SALE_AMT
    FROM STAGE.INVOICE i
    JOIN STAGE.INVOICELINE il ON i."InvoiceId" = il."InvoiceId"
    JOIN DW.CUSTOMER_DIM cd ON cd.CUSTOMER_ID = i."CustomerId"
    GROUP BY i."InvoiceId", cd.CUSTOMER_KEY, i."InvoiceDate"
)
SELECT 
    SALES_FACT_SEQ.NEXTVAL AS SALES_KEY,
    a.CUSTOMER_KEY,
    a.INVOICE_ID,
    dd.DATE_KEY AS DATE_DIM_KEY,
    td.TIME_KEY AS TIME_DIM_KEY,
    a.TOTAL_SALE_AMT,
    'ADF_PIPELINE' AS SOURCE_ID,
    CURRENT_TIMESTAMP() AS DATE_TO_WAREHOUSE
FROM aggsales a
JOIN DW.DATE_DIM dd ON dd.FULL_DATE = DATE(a.SALE_DATE)
JOIN DW.TIME_DIM td ON td.TIME_24_HR = TO_CHAR(a.SALE_DATE, 'HH24:MI')
LEFT JOIN DW.SALES_FACT sf ON sf.INVOICE_ID = a.INVOICE_ID
WHERE sf.INVOICE_ID IS NULL;
```

---

## ✔️ Data Validation

**Extract Validation:**
```sql
-- Check Azure SQL row counts
SELECT COUNT(*) FROM chinook.Invoice;

-- Check Parquet file row counts
SELECT COUNT(*) FROM STAGE.INVOICE;
-- Should match
```

**Stage Validation:**
```sql
-- Verify audit columns populated
SELECT Created_By, Created_Dt FROM STAGE.INVOICE LIMIT 5;
-- Should show: 'ADF_PIPELINE', <current_date>
```

**Dimension Validation:**
```sql
-- Check surrogate keys generated
SELECT COUNT(DISTINCT CUSTOMER_KEY) FROM DW.CUSTOMER_DIM;
SELECT COUNT(DISTINCT CUSTOMER_ID) FROM STAGE.CUSTOMER;
-- Should match
```

**Fact Validation:**
```sql
-- Row count check
SELECT COUNT(*) FROM DW.SALES_FACT;
SELECT COUNT(DISTINCT "InvoiceId") FROM STAGE.INVOICE;
-- Should match

-- Amount reconciliation
SELECT SUM(TOTAL_SALE_AMT) FROM DW.SALES_FACT;
SELECT SUM("Quantity" * "UnitPrice") FROM STAGE.INVOICELINE;
-- Should match

-- Referential integrity
SELECT COUNT(*) 
FROM DW.SALES_FACT sf
LEFT JOIN DW.CUSTOMER_DIM cd ON sf.CUSTOMER_KEY = cd.CUSTOMER_KEY
WHERE cd.CUSTOMER_KEY IS NULL;
-- Should return 0
```

---

## 💡 Key Challenges & Solutions

| Challenge | Solution |
|-----------|----------|
| **Azure SQL to Parquet export** | Used ADF Copy Activity with Parquet sink and Snappy compression |
| **Column name case mismatch** | Added Select transformation to convert Parquet mixed case → Snowflake UPPERCASE |
| **Schema mismatch with audit columns** | Used Data Flow Derived Column transformation to add columns dynamically |
| **Date/Time dimension lookups** | Pre-populated DATE_DIM and TIME_DIM, joined during fact load |
| **Incremental loading** | LEFT JOIN anti-pattern: `WHERE sf.INVOICE_ID IS NULL` |
| **Query timeout in Copy Activity** | Alternative: Moved complex queries to Snowflake stored procedures |

---

## 📚 References

- [Azure SQL Database Documentation](https://learn.microsoft.com/en-us/azure/azure-sql/?view=azuresql)
- [Azure Blob Storage Documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/)
- [Azure Data Factory Documentation](https://docs.microsoft.com/en-us/azure/data-factory/)
- [Snowflake Documentation](https://docs.snowflake.com/)
