
---

# 🚀 Enterprise Data Engineering Pipeline

**Azure Databricks | Apache Spark | Delta Lake | Power BI**

---

## 📌 Project Overview

This project implements an **end-to-end enterprise data engineering pipeline** using **Azure Databricks** and **Delta Live Tables (DLT)**.
It ingests raw transactional data, processes it incrementally, applies data quality and calibration rules, and delivers analytics-ready datasets for reporting in **Power BI**.

The solution follows the **Medallion Architecture (Bronze → Silver → Gold)** to ensure scalability, reliability, and data quality.

---

## 🏗️ Architecture Overview

```
Source CSV Files
     ↓
Bronze Layer (Raw Ingestion)
     ↓
Silver Layer (Cleansing, Validation, Calibration)
     ↓
Gold Layer (Business Aggregations)
     ↓
Power BI Dashboards
```

---

## 🧰 Technologies Used

* **Azure Databricks** – Distributed processing & orchestration
* **Apache Spark (PySpark)** – Scalable transformations
* **Delta Lake** – ACID storage, schema enforcement, time travel
* **Delta Live Tables (DLT)** – Declarative pipeline management
* **Power BI** – Business intelligence & visualization

---

## 🗂️ Data Layers

### 🟤 Bronze Layer – Raw Data

* Stores raw source data without modifications
* Append-only ingestion
* Includes ingestion metadata for traceability

**Tables**

* `bronze_sales_transactions`
* `bronze_product_master`
* `bronze_store_region`

---

### ⚪ Silver Layer – Cleaned & Calibrated Data

* Deduplicates records
* Handles nulls and invalid values
* Applies data quality rules
* Normalizes currency to USD
* Routes invalid records to quarantine

**Tables**

* `silver_sales_transactions`
* `silver_product_master`
* `silver_store_region`
* `silver_sales_quarantine`

---

### 🟡 Gold Layer – Analytics-Ready Data

* Business-level aggregations
* Optimized for reporting
* Consumed directly by Power BI

**Datasets**

* `gold_daily_sales_summary`
* `gold_monthly_revenue_by_region`
* `gold_product_performance`

---

## 🔁 Incremental Processing Strategy

* **Bronze**: Append-only ingestion of new data
* **Silver**: MERGE-based upserts using DLT `apply_changes`
* **Gold**: Deterministic aggregations derived from Silver

**Benefits**

* Faster pipeline execution
* Lower compute cost
* Safe reruns without duplication

---

## 🎯 Data Quality & Calibration

### Validation Rules

* Quantity > 0
* Unit price > 0
* Valid product and store IDs

### Calibration Logic

```
total_amount = quantity × unit_price − discount
```

### Currency Normalization

* All monetary values converted to **USD**
* Ensures consistent reporting across regions

### Quarantine Handling

* Invalid records stored in `silver_sales_quarantine`
* Rejection reasons logged for auditing

---

## 📊 Power BI Dashboards

### Executive Sales Dashboard

* Total revenue
* Revenue trends
* Regional breakdown

### Product Performance Dashboard

* Top products
* Category-wise revenue
* Monthly trends

Dashboards connect directly to **Gold datasets** using the **Azure Databricks connector**.

---

## 📈 Logging, Monitoring & Error Handling

### Logging

* Pipeline execution events via DLT event logs
* Record counts per layer logged in Delta tables
* Rejected records tracked with rejection reasons

### Error Handling

* Automatic failure detection via DLT
* Idempotent pipeline reruns
* No data duplication on retries

---

## 🔒 Security & Enterprise Considerations

* Role-based access control via Databricks
* Environment separation (Dev / Test / Prod)
* Data lineage through DLT dependency tracking

---

## ▶️ How to Run the Project

1. Upload source CSV files to Databricks storage
2. Configure and run the DLT pipeline
3. Execute the logging & monitoring job
4. Connect Power BI to Gold datasets
5. Refresh dashboards to view insights

---

## 📌 Key Highlights

* End-to-end automated pipeline
* Incremental & idempotent processing
* Strong data quality enforcement
* Analytics-ready reporting layer
* Enterprise-ready architecture

---
