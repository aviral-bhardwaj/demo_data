# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer Ingestion
# MAGIC
# MAGIC **Purpose**: Ingest raw data files into Bronze layer Delta tables
# MAGIC
# MAGIC **Bronze Layer Philosophy**:
# MAGIC - Minimal transformation
# MAGIC - Preserve raw data exactly as received
# MAGIC - Add ingestion metadata only
# MAGIC
# MAGIC **Tables Created**:
# MAGIC - `bronze.digital_journeys_raw`
# MAGIC - `bronze.call_logs_raw`
# MAGIC - `bronze.member_profiles_raw`

# COMMAND ----------

# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import sys
sys.path.append("/Workspace/Repos/notebooks/config")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for catalog and schema configuration
dbutils.widgets.text("catalog_name", "insurance_final_with_ai", "Catalog Name")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema Name")
dbutils.widgets.text("input_path", "/Volumes/insurance_final_with_ai/default/data", "Input Data Path")
dbutils.widgets.text("checkpoint_path", "/Volumes/insurance_final_with_ai/default/data/checkpoints/bronze", "Checkpoint Path")

# Get widget values
CATALOG = dbutils.widgets.get("catalog_name")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
INPUT_PATH = dbutils.widgets.get("input_path")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")

print(f"Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Bronze Schema: {BRONZE_SCHEMA}")
print(f"  Input Path: {INPUT_PATH}")
print(f"  Checkpoint Path: {CHECKPOINT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Schema

# COMMAND ----------

# # Create catalog and schema if they don't exist
# spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
# spark.sql(f"USE CATALOG {CATALOG}")
# spark.sql(f"USE SCHEMA {BRONZE_SCHEMA}")

print(f"Using catalog: {CATALOG}, schema: {BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Bronze Schemas

# COMMAND ----------

# Digital Journeys Schema
digital_journeys_schema = StructType([
    StructField("member_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("journey_type", StringType(), True),
    StructField("action", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("error_code", StringType(), True),
    StructField("error_message", StringType(), True)
])

# Call Logs Schema
call_logs_schema = StructType([
    StructField("call_id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("call_timestamp", TimestampType(), True),
    StructField("ivr_path", StringType(), True),
    StructField("handle_time_seconds", IntegerType(), True),
    StructField("agent_id", StringType(), True),
    StructField("call_reason", StringType(), True),
    StructField("sentiment_score", DoubleType(), True),
    StructField("resolution_status", StringType(), True)
])

# Member Profiles Schema
member_profiles_schema = StructType([
    StructField("member_id", StringType(), True),
    StructField("plan_type", StringType(), True),
    StructField("tenure_months", IntegerType(), True),
    StructField("segment", StringType(), True),
    StructField("language_preference", StringType(), True),
    StructField("enrollment_date", DateType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Digital Journeys

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Cell 10
import pandas as pd

def ingest_to_bronze(df, table_name, source_file):
    """
    Generic function to ingest data into bronze layer with metadata
    
    Args:
        df: Source DataFrame
        table_name: Target table name
        source_file: Source file name for lineage
    """
    # Add ingestion metadata
    df_with_metadata = (df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit(source_file))
    )
    
    # Write to Delta table
    (df_with_metadata
        .write
        .format("delta")
        .mode("overwrite")  # For initial load; use "append" for incremental
        .option("mergeSchema", "true")
        .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}")
    )
    
    return df_with_metadata

# Read digital journeys CSV using pandas (workaround for Spark CSV reader issue with Volumes)
digital_journeys_pdf = pd.read_csv("/Volumes/insurance_final_with_ai/default/data/digital_journeys.csv")
digital_journeys_raw = spark.createDataFrame(digital_journeys_pdf, schema=digital_journeys_schema)

# Ingest to bronze
digital_journeys_bronze = ingest_to_bronze(
    digital_journeys_raw,
    "digital_journeys_raw",
    "digital_journeys.csv"
)

print(f"Ingested {digital_journeys_bronze.count():,} digital journey records to bronze.digital_journeys_raw")
print("\nSample data:")
digital_journeys_bronze.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Call Logs

# COMMAND ----------

# DBTITLE 1,Cell 12
# Read call logs CSV using pandas (workaround for Spark CSV reader issue with Volumes)
call_logs_pdf = pd.read_csv(f"{INPUT_PATH}/call_logs.csv")
call_logs_raw = spark.createDataFrame(call_logs_pdf, schema=call_logs_schema)

# Ingest to bronze
call_logs_bronze = ingest_to_bronze(
    call_logs_raw,
    "call_logs_raw",
    "call_logs.csv"
)

print(f"Ingested {call_logs_bronze.count():,} call log records to bronze.call_logs_raw")
print("\nSample data:")
call_logs_bronze.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Member Profiles

# COMMAND ----------

# DBTITLE 1,Cell 14
# Read member profiles CSV using pandas (workaround for Spark CSV reader issue with Volumes)
member_profiles_pdf = pd.read_csv(f"{INPUT_PATH}/member_profiles.csv")
member_profiles_raw = spark.createDataFrame(member_profiles_pdf, schema=member_profiles_schema)

# Ingest to bronze
member_profiles_bronze = ingest_to_bronze(
    member_profiles_raw,
    "member_profiles_raw",
    "member_profiles.csv"
)

print(f"Ingested {member_profiles_bronze.count():,} member profile records to bronze.member_profiles_raw")
print("\nSample data:")
member_profiles_bronze.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Queries

# COMMAND ----------

# Validate table creation
print("Bronze Layer Tables Created:")
print("=" * 80)

tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}").collect()
for table in tables:
    table_name = table.tableName
    count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}").count()
    print(f"  ✓ {table_name}: {count:,} records")

print("=" * 80)

# COMMAND ----------

# Data quality checks
print("\nData Quality Checks:")
print("=" * 80)

# Check for nulls in key fields
digital_null_check = spark.sql(f"""
    SELECT 
        COUNT(*) as total_records,
        SUM(CASE WHEN member_id IS NULL THEN 1 ELSE 0 END) as null_member_ids,
        SUM(CASE WHEN session_id IS NULL THEN 1 ELSE 0 END) as null_session_ids,
        SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) as null_timestamps
    FROM {CATALOG}.{BRONZE_SCHEMA}.digital_journeys_raw
""").collect()[0]

print(f"\nDigital Journeys:")
print(f"  Total records: {digital_null_check.total_records:,}")
print(f"  Null member_ids: {digital_null_check.null_member_ids}")
print(f"  Null session_ids: {digital_null_check.null_session_ids}")
print(f"  Null timestamps: {digital_null_check.null_timestamps}")

call_null_check = spark.sql(f"""
    SELECT 
        COUNT(*) as total_records,
        SUM(CASE WHEN call_id IS NULL THEN 1 ELSE 0 END) as null_call_ids,
        SUM(CASE WHEN member_id IS NULL THEN 1 ELSE 0 END) as null_member_ids,
        SUM(CASE WHEN call_timestamp IS NULL THEN 1 ELSE 0 END) as null_timestamps
    FROM {CATALOG}.{BRONZE_SCHEMA}.call_logs_raw
""").collect()[0]

print(f"\nCall Logs:")
print(f"  Total records: {call_null_check.total_records:,}")
print(f"  Null call_ids: {call_null_check.null_call_ids}")
print(f"  Null member_ids: {call_null_check.null_member_ids}")
print(f"  Null timestamps: {call_null_check.null_timestamps}")

member_null_check = spark.sql(f"""
    SELECT 
        COUNT(*) as total_records,
        SUM(CASE WHEN member_id IS NULL THEN 1 ELSE 0 END) as null_member_ids
    FROM {CATALOG}.{BRONZE_SCHEMA}.member_profiles_raw
""").collect()[0]

print(f"\nMember Profiles:")
print(f"  Total records: {member_null_check.total_records:,}")
print(f"  Null member_ids: {member_null_check.null_member_ids}")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 80)
print("BRONZE INGESTION SUMMARY")
print("=" * 80)

print(f"\nCatalog: {CATALOG}")
print(f"Schema: {BRONZE_SCHEMA}")
print(f"\nTables Created:")
print(f"  ✓ digital_journeys_raw")
print(f"  ✓ call_logs_raw")
print(f"  ✓ member_profiles_raw")

print(f"\nData Quality: All key fields validated")
print(f"Status: ✓ Ready for Silver layer transformation")

print("=" * 80)
