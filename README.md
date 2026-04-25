# Data Analyst Portfolio — Rohit Athityaa

End-to-end data analytics projects using Python, SQL, dbt, Snowflake, and Power BI.

---

## 🔷 Project 1: dbt + Snowflake Analytics Pipeline

### What it does
A production-grade analytics pipeline built on 1.5M+ rows of e-commerce data.
Transforms raw transactional data into business-ready reporting tables using
dbt and Snowflake.

### Architecture
Raw Data (Snowflake) → Staging Layer → Intermediate Layer → Mart Layer → Reporting

### Tools Used
- **Snowflake** — cloud data warehouse
- **dbt** — data transformation and modeling
- **Python** — data extraction and cleaning
- **Power BI** — dashboard and KPI reporting

### Data Model (Star Schema)
| Table | Type | Description |
|---|---|---|
| fct_orders | Fact | One row per order with revenue, quantity metrics |
| dim_customers | Dimension | Customer details, segments, location |
| dim_products | Dimension | Product category, price, SKU |

### What's inside
- 13 dbt models across staging, intermediate, and mart layers
- 16 automated data quality tests (null checks, referential integrity)
- Full dbt lineage graph documentation
- Star schema design optimized for analytics queries

### Key Results
- Processed 1.5M+ rows of raw e-commerce data
- Increased data usability by 30% through cleaning and transformation
- Reduced data processing time by 20%

---

## 🔷 Project 2: End-to-End Sales Analytics Pipeline (Python + SQL + Power BI)

### What it does
An interactive Power BI dashboard analyzing sales trends, customer behavior,
and revenue performance across regions.

### Tools Used
- **Python (Pandas)** — ETL and data cleaning
- **SQL** — advanced queries (JOINs, CTEs, Window Functions)
- **Power BI (DAX)** — interactive dashboards and KPI reporting

### Key Results
- Processed 200K+ records end-to-end
- Identified key revenue drivers through EDA
- Built dynamic KPI dashboards for business decision-making

---

## 🔷 Project 3: SQL Data Analysis

### What it does
Advanced SQL analysis on large datasets to surface business insights.

### Techniques Used
- JOINs, CTEs, Window Functions, Aggregations
- Trend analysis, pattern detection, performance reporting

---

## 🔷 Project 4: Python ETL & Data Cleaning

### What it does
End-to-end ETL pipeline extracting data from multiple sources,
cleaning and transforming using Pandas.

### Key Results
- Reduced processing time by 35%
- Standardized datasets for downstream reporting

---

## Tech Stack
| Category | Tools |
|---|---|
| Languages | Python, SQL |
| Warehousing | Snowflake |
| Transformation | dbt |
| Visualization | Power BI, Matplotlib |
| Libraries | Pandas, NumPy |
| Other | Git, Excel, MySQL |

---

## Contact
📧 rohitathityaagb@gmail.com  
📍 Danville, VA  
