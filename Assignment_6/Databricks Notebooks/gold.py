# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# Gold Daily Sales Summary - Aggregated Sales Metrics by Date and Region

dlt.create_table(
    name="gold_daily_sales_summary",
    comment="Daily aggregated sales metrics",
    table_properties={"quality": "gold"}
)

@dlt.table
def gold_daily_sales_summary():

    df = dlt.read("silver_sales_transactions")
    store_df = dlt.read("silver_store_region")
    return (
        df
        .join(store_df, "store_id", "inner")
        .withColumn("sales_date", to_date("transaction_timestamp"))
        .groupBy("sales_date","region")
        .agg(
            countDistinct("transaction_id").alias("total_transactions"),
            sum("quantity").alias("total_quantity"),
            sum("total_amount_usd").alias("total_revenue_usd")
        )
    )


# COMMAND ----------

# Gold Monthly Revenue by Region - Aggregated Revenue Metrics by Month and Region

dlt.create_table(
    name="gold_monthly_revenue_by_region",
    comment="Monthly revenue aggregated by region",
    table_properties={"quality": "gold"}
)

@dlt.table
def gold_monthly_revenue_by_region():

    sales_df = dlt.read("silver_sales_transactions")
    store_df = dlt.read("silver_store_region")

    return (
        sales_df
        .join(store_df, "store_id", "inner")
        .withColumn("year", year("transaction_timestamp"))
        .withColumn("month", month("transaction_timestamp"))
        .groupBy("year", "month", "region")
        .agg(
            sum("total_amount_usd").alias("monthly_revenue_usd"),
            countDistinct("transaction_id").alias("transactions_count")
        )
    )


# COMMAND ----------

# Silver Product Performance - Aggregated Product Metrics

dlt.create_table(
    name="gold_product_performance",
    comment="Product-level performance metrics",
    table_properties={"quality": "gold"}
)

@dlt.table
def gold_product_performance():

    sales_df = dlt.read("silver_sales_transactions")
    product_df = dlt.read("silver_product_master")

    return (
        sales_df
        .join(product_df, "product_id", "inner")
        .groupBy(
            "product_id",
            "product_name",
            "category",
            "brand"
        )
        .agg(
            sum("quantity").alias("total_units_sold"),
            sum("total_amount_usd").alias("total_revenue_usd"),
            countDistinct("transaction_id").alias("transactions_count")
        )
    )
