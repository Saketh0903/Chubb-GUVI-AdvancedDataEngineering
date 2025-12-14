
---

# 📊 Sales Performance & Trend Analysis Dashboard (Power BI)

## Overview

An interactive **Power BI dashboard** for analyzing **Sales, Profit, and Quantity**, tracking trends over time, and comparing performance across regions, categories, and products.
The model uses **direct Fact and Dimension tables** with Excel as the **single source of truth**.

---

## Data Sources

* **FactSales (Excel)** – Transaction-level sales data
* **DimDate (Calendar table)**
* **DimProduct (Excel)**
* **DimCustomer (Excel)**
* **DimRegion (Excel)**
* **Targets (Excel)**

---

## Data Model

* **Star schema**
* One-to-many relationships (Dimensions → FactSales)
* Single-direction filters
* DimDate marked as **Date Table**

---

## Key Measures (DAX)

```DAX
Total Sales = SUM(FactSales[SalesAmount])
Total Profit = SUM(FactSales[Profit])
Total Quantity = SUM(FactSales[Quantity])

Sales YTD = TOTALYTD([Total Sales], DimDate[Date])
Sales LY = CALCULATE([Total Sales], SAMEPERIODLASTYEAR(DimDate[Date]))
YoY Growth % = DIVIDE([Total Sales] - [Sales LY], [Sales LY])
```

---

## Report Pages

* **Executive Dashboard** – KPIs, category split, sales trend
* **Trend Analysis** – Sales & profit over time, regional view
* **Product Performance** – Category → Product matrix with Top N

---

## Features

* Report-level filters (Year, Region, Category)
* Conditional formatting in matrix
* Dashboard actions & drill-through
* KPI alerts in Power BI Service
* R Script visual for trend analysis

---

## Tools

* Power BI Desktop & Service
* Power Query
* DAX
* Microsoft Excel
* R

---

