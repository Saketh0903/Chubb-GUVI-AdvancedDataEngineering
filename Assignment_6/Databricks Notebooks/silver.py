# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Creation of streaming tables

dlt.create_streaming_table(
    name="silver_sales_transactions",
    comment="Silver sales facts cleaned, calibrated, USD-normalized",
    table_properties={"quality": "silver"}
)

dlt.create_streaming_table(
    name="silver_product_master",
    comment="Silver product master cleaned via SCD Type 1",
    table_properties={"quality": "silver"}
)

dlt.create_streaming_table(
    name="silver_store_region",
    comment="Silver store master cleaned via SCD Type 1",
    table_properties={"quality": "silver"}
)


# COMMAND ----------

# Stage for silver_sales_transactions

@dlt.view
def silver_sales_staging():

    df = (
        dlt.read_stream("bronze_sales_transactions")
        .withWatermark("ingestion_timestamp", "1 day")
    )

    df = (
        df
        .withColumn("transaction_timestamp", to_timestamp("transaction_timestamp"))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("unit_price", col("unit_price").cast(DecimalType(10,2)))
        .withColumn("discount", coalesce(col("discount"), lit(0)).cast(DecimalType(10,2)))
        .withColumn("total_amount", col("total_amount").cast(DecimalType(12,2)))
    )

    # Mandatory calibration
    df = df.withColumn(
        "calculated_total",
        (col("quantity") * col("unit_price")) - col("discount")
    )

    # Currency normalization → USD
    df = df.withColumn(
        "fx_rate_to_usd",
        when(col("currency") == "USD", lit(1.0))
        .when(col("currency") == "INR", lit(0.012))
        .when(col("currency") == "EUR", lit(1.10))
        .when(col("currency") == "GBP", lit(1.25))
    ).withColumn(
        "total_amount_usd",
        round(col("calculated_total") * col("fx_rate_to_usd"), 2)
    )

    return df


# COMMAND ----------

# Silver Sales Transactions

dlt.apply_changes(
    target="silver_sales_transactions",
    source="silver_sales_staging",
    keys=["transaction_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1,
    ignore_null_updates=True
)


# COMMAND ----------

# Staging for silver_product_master

@dlt.view
def silver_product_staging():

    df = dlt.read_stream("bronze_product_master")

    return (
        df
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("product_name", initcap(col("product_name")))
        .withColumn("category", initcap(col("category")))
        .withColumn("brand", initcap(col("brand")))
        .withColumn("unit_price", col("unit_price").cast("decimal(10,2)"))
        .filter(col("product_id").isNotNull())
        .filter(col("unit_price") > 0)
    )

# COMMAND ----------

# Silver Product Master

dlt.apply_changes(
    target="silver_product_master",
    source="silver_product_staging",
    keys=["product_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1,
    ignore_null_updates=True
)


# COMMAND ----------

# Staging for silver_store_region

@dlt.view
def silver_store_staging():

    df = dlt.read_stream("bronze_store_region")

    return (
        df
        .withColumn("store_id", trim(col("store_id")))
        .withColumn("store_name", initcap(col("store_name")))
        .withColumn("country", upper(col("country")))
        .withColumn("region", upper(col("region")))
        .filter(col("store_id").isNotNull())
    )


# COMMAND ----------

# Silver Store Region

dlt.apply_changes(
    target="silver_store_region",
    source="silver_store_staging",
    keys=["store_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1,
    ignore_null_updates=True
)


# COMMAND ----------

# Sales Quarantine Table

@dlt.table(
    name="silver_sales_quarantine",
    comment="Invalid sales records with rejection reasons",
    table_properties={"quality": "quarantine"}
)
def silver_sales_quarantine():

    df = dlt.read_stream("silver_sales_staging")

    return (
        df
        .withColumn(
            "rejection_reason",
            when(col("quantity") <= 0, "INVALID_QUANTITY")
            .when(col("unit_price") <= 0, "INVALID_UNIT_PRICE")
            .when(col("fx_rate_to_usd").isNull(), "UNSUPPORTED_CURRENCY")
            .when(col("product_id").isNull(), "INVALID_PRODUCT_ID")
            .when(col("store_id").isNull(), "INVALID_STORE_ID")
        )
        .filter(col("rejection_reason").isNotNull())
    )
