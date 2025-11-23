
---

# **README**

## **Input Data**
### **`bronze_taxi_trips`**  
A **streaming table** containing raw NYC taxi trip data.  
It includes fields such as pickup/dropoff timestamps, trip distance, fare, and location IDs.  
This table stores the data exactly as it arrives, with only minimal schema enforcement.

---

## **Transformations**

### **1. Silver Layer**
#### **`silver_flagged_rides`**  
- Cleans and normalizes trip data  
- Applies basic validation rules  
- Flags rides with potential issues (e.g., unrealistic distances, fares, or missing values)

#### **`silver_weekly_stats`**  
- Aggregates taxi trips by week  
- Computes metrics such as total rides, average fare, and average distance  
- Produces summary statistics used for reporting

---

### **2. Gold Layer**
#### **`gold_top_n`**  
- Combines outputs from the Silver tables  
- Produces a top-N summary (such as busiest zones or highest-volume weeks)  
- Intended for final analytics and dashboards

---

## **Notebook Files**

### **`NYTaxi Pipeline SQL.ipynb`**
Defines the Delta Live Tables pipeline using SQL.  
Includes:
- Data Ingestion from streaming sources
- Transformation rules  
- Expectations and data quality constraints  

### **`Validation.ipynb`**
Runs validation checks on the output tables.  
Includes:
- Record count checks  
- Schema validation  
- Basic sanity checks for the Silver and Gold outputs  

---
