# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# Bronze Sales Transactions

@dlt.table(
    name="bronze_sales_transactions",
    table_properties={"quality": "bronze"},
    partition_cols=["ingestion_date"]
)
def bronze_sales_transactions():

    df = (
        spark.readStream
        .format("cloudFiles")            
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/dlt_schemas/sales/")
        .option("header", "true")
        .load("/Volumes/workspace/default/input_data/sales/")
    )

    return (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", current_date())
        .withColumn("source_system", lit("us_retail_csv"))
    )


# COMMAND ----------

# Bronze Product Master

import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="bronze_product_master",
    comment="Bronze layer - incremental raw product master data",
    table_properties={"quality": "bronze"},
    partition_cols=["ingestion_date"]
)
def bronze_product_master():

    df = (
        spark.readStream
        .format("cloudFiles")           
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/dlt_schemas/products/")
        .option("header", "true")
        .load("/Volumes/workspace/default/input_data/products/")
    )

    return (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", current_date())
        .withColumn("source_system", lit("us_retail_csv"))
    )


# COMMAND ----------

# Bronze Store Region

@dlt.table(
    name="bronze_store_region",
    comment="Bronze layer - incremental raw store & region data",
    table_properties={"quality": "bronze"},
    partition_cols=["ingestion_date"]
)
def bronze_store_region():

    df = (
        spark.readStream
        .format("cloudFiles")                       # 👈 incremental
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/dlt_schemas/stores/")
        .option("header", "true")
        .load("/Volumes/workspace/default/input_data/stores/")
    )

    return (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", current_date())
        .withColumn("source_system", lit("us_retail_csv"))
    )
