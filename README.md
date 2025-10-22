# apple-retail-pipeline

## Overview
This project demonstrates an end-to-end **Data Engineering ETL pipeline** built to process Apple Retail Sales data.  
The pipeline extracts, transforms, loads, and analyzes sales data from multiple dimensions (stores, products, and categories) to enable analytical insights such as category performance, store-level revenue, and product trends.

The workflow follows a modular and production-ready structure that simulates how enterprise data pipelines operate in real-world analytics systems.

---

## Architecture

**1. Data Sources**  
Raw CSV/Parquet files representing Apple retail sales transactions and reference tables (stores, products, categories).

**2. Extraction (Extract Layer)**  
- Files are stored in the `/data/raw/` directory.  
- Extract scripts identify the latest files automatically.  
- Optionally uploaded to **Azure Data Lake Gen2** for cloud-based storage using `azure-storage-file-datalake`.

**3. Transformation (Transform Layer)**  
- Raw data is cleaned and normalized using **Pandas**.  
- Data types are standardized (dates, decimals, and IDs).  
- Datasets are joined with dimension tables to enrich sales facts.  
- The transformed files are stored in `/data/processed/`.

**4. Loading (Load Layer)**  
- Processed data is loaded into a **DuckDB** database for analysis.  
- Schema follows a **Star Schema** design:
  - `fact_sales`
  - `dim_product`
  - `dim_store`
  - `dim_category`

**5. Analysis**  
Exploratory queries and aggregations are performed in DuckDB to analyze sales performance, including:
- Category-wise revenue and averages  
- Store-level category performance  
- Top-selling categories per store  

---

## Tech Stack

| Component | Technology |
|------------|-------------|
| Language | Python |
| Storage | Azure Data Lake Gen2 |
| Query Engine | DuckDB |
| Data Processing | Pandas |
| Orchestration | (Optional) Apache Airflow |
| File Formats | CSV, Parquet |
| Environment | Virtualenv (`venv`) |
| Visualization | Pandas, Matplotlib (optional) |

---

## Folder Structure
```
apple-retail-pipeline/
│
├── data/
│   ├── raw/               # Raw sales and dimension data
│   ├── processed/         # Cleaned and enriched data
│   └── analysis/          # Query outputs and visualizations
│
├── src/
│   ├── extract.py         # Extract raw data from local/ADLS
│   ├── transform.py       # Clean and join datasets
│   ├── load.py            # Load processed data into DuckDB
│   ├── inspect_warehouse.py # Preview loaded tables
│   └── utils/
│       ├── io_datalake.py # Functions for Azure Data Lake operations
│       └── helpers.py
│
├── .env                   # Environment variables (ADLS credentials)
├── requirements.txt
├── docker-compose.yaml    # (Optional) for orchestration
└── README.md
```

---

## Sample Analytical Queries

### Q1 — Total Revenue by Store
```sql
SELECT s.store_name, SUM(f.revenue) AS total_revenue
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY s.store_name
ORDER BY total_revenue DESC;
```

### Q2 — Category-wise Total and Average Revenue
```sql
SELECT c.category_name, 
       SUM(f.revenue) AS total_revenue, 
       AVG(f.revenue) AS avg_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_category c ON p.category_id = c.category_id
GROUP BY c.category_name
ORDER BY total_revenue DESC;
```

### Q3 — Top-Selling Category per Store
```sql
WITH category_revenue AS (
    SELECT s.store_name, c.category_name, SUM(f.revenue) AS total_revenue
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_category c ON p.category_id = c.category_id
    JOIN dim_store s ON f.store_id = s.store_id
    GROUP BY s.store_name, c.category_name
)
SELECT store_name, category_name, total_revenue
FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY store_name ORDER BY total_revenue DESC) AS rank
    FROM category_revenue
)
WHERE rank = 1;
```

---

## Key Learnings
- Building modular ETL pipelines in Python  
- Implementing a **star schema** for analytical queries  
- Managing data storage in **Azure Data Lake Gen2**  
- Efficient querying and transformation with **DuckDB**  
- Designing real-world analytical questions with SQL  