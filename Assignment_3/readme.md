# **ShopEZ Delta Lake (Databricks)**

Delta Lake storage for ShopEZ e-commerce orders with partitioning, ACID guarantees, Time Travel, schema evolution, and updates/deletes.

---

## **Data Model**

* Base: `order_id`, `order_timestamp`, `customer_id`, `country`, `amount`, `currency`, `status`
* Derived: `order_date`
* Evolved: `payment_method`, `coupon_code`

---

## **Design**

* **Format:** Delta
* **Partitions:** `country`, `order_date`
* **Storage:** Cloud object store (DBFS/S3/ADLS)
* **Benefits:** Partition pruning, ACID, versioning, schema evolution

Example layout:

```
/country=US/order_date=2025-11-28/
/country=IN/order_date=2025-11-29/
/_delta_log/
```

---

## **Workflow**

1. Ingest daily CSV/JSON files
2. Derive `order_date`
3. Write as Delta with partitioning
4. Verify partitions
5. Query using filters for pruning
6. Use Time Travel for older versions
7. Allow schema evolution for new fields
8. Perform updates & deletes
9. (Optional) Run OPTIMIZE + Z-ORDER
10. Mitigate small files with repartitioning & OPTIMIZE

---

