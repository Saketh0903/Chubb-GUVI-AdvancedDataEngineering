# **README – ShopVerse Daily ETL Pipeline**

## **1. Overview**

A daily Airflow ETL pipeline processes customer, product, and order data for ShopVerse.
It ingests files from a landing directory, loads staging tables, builds dimension and fact tables, applies data quality checks, and triggers alerts on failure.
Pipeline supports **backfilling**, **branching**, and **anomaly logging**.

---

## **2. Required Airflow Setup**

### **Variables**

| Key                             | Example             | Purpose                    |
| ------------------------------- | ------------------- | -------------------------- |
| `shopverse_data_base_path`      | `/opt/airflow/data` | Landing directory root     |
| `shopverse_sql_base_path`       | `/opt/airflow/sql`  | SQL scripts folder         |
| `shopverse_min_order_threshold` | `10`                | Low-volume alert threshold |

### **Connections**

| Conn ID                   | Type         | Purpose                |
| ------------------------- | ------------ | ---------------------- |
| `postgres_dwh`            | Postgres     | Staging + warehouse DB |
| `fs_default`              | FileSystem   | Used by FileSensors    |
| `slack_alerts` | HTTP/Webhook | Failure alerts         |

---

## **3. Input Files**

Placed under:

```
/opt/airflow/data/landing/customers/customers_YYYYMMDD.csv
/opt/airflow/data/landing/products/products_YYYYMMDD.csv
/opt/airflow/data/landing/orders/orders_YYYYMMDD.json
```

Files correspond to **previous day's data** for each DAG run.

---

## **4. Pipeline Logic**

### **Schedule**

* Runs daily at **01:00**, processes **yesterday's** data.

### **Flow**

1. **File Sensors** wait for all three datasets.
2. **Staging TaskGroup**

   * Truncate staging tables
   * Load CSV/JSON into `stg_customers`, `stg_products`, `stg_orders`
3. **Warehouse TaskGroup**

   * Upsert dims (`dim_customers`, `dim_products`)
   * Build `fact_orders`
   * Includes dedupe, invalid-row removal, UTC conversion, currency check
4. **Data Quality Checks**

   * `dim_customers` > 0
   * No NULL FKs in fact
   * Fact count matches validated staging count
5. **Branching**

   * If fact count < threshold → write anomaly + warning branch
   * Else → normal completion
6. **Alerts**

   * Slack alert (if enabled) fires on any upstream failure.

---


