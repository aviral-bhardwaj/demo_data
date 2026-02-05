# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Layer Transformation
# MAGIC 
# MAGIC **Purpose**: Clean, enrich and join data to identify digital-to-call leakage
# MAGIC 
# MAGIC **Silver Layer Philosophy**:
# MAGIC - Data cleansing (handle nulls, duplicates)
# MAGIC - Data type standardization
# MAGIC - Business logic application
# MAGIC - Enrichment with derived fields
# MAGIC 
# MAGIC **Tables Created**:
# MAGIC - `silver.digital_journeys_cleaned`
# MAGIC - `silver.call_logs_cleaned`
# MAGIC - `silver.member_profiles_enriched`
# MAGIC - `silver.digital_call_leakage` (main analytical table)

# COMMAND ----------

# Import required libraries
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
from datetime import datetime
import sys
sys.path.append("/Workspace/Repos/notebooks/config")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for configuration
dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema Name")
dbutils.widgets.text("silver_schema", "silver", "Silver Schema Name")
dbutils.widgets.text("leakage_window_minutes", "30", "Leakage Window (minutes)")

# Get widget values
CATALOG = dbutils.widgets.get("catalog_name")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
LEAKAGE_WINDOW_MINUTES = int(dbutils.widgets.get("leakage_window_minutes"))

print(f"Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Bronze Schema: {BRONZE_SCHEMA}")
print(f"  Silver Schema: {SILVER_SCHEMA}")
print(f"  Leakage Window: {LEAKAGE_WINDOW_MINUTES} minutes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Schema

# COMMAND ----------

# Create silver schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")

print(f"Using catalog: {CATALOG}")
print(f"Silver schema created/verified: {SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Tables

# COMMAND ----------

# Load bronze tables
digital_journeys_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.digital_journeys_raw")
call_logs_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.call_logs_raw")
member_profiles_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.member_profiles_raw")

print(f"Loaded bronze tables:")
print(f"  - digital_journeys_raw: {digital_journeys_bronze.count():,} records")
print(f"  - call_logs_raw: {call_logs_bronze.count():,} records")
print(f"  - member_profiles_raw: {member_profiles_bronze.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Member Profiles

# COMMAND ----------

# Clean and enrich member profiles
member_profiles_silver = (member_profiles_bronze
    .filter(F.col("member_id").isNotNull())  # Remove nulls
    .dropDuplicates(["member_id"])  # Remove duplicates
    .withColumn("processing_timestamp", F.current_timestamp())
    # Derive member_category (used for segment analysis)
    .withColumn("member_category",
        F.when((F.col("plan_type") == "MAPD") & (F.col("segment") == "new"), F.lit("MAPD New"))
        .when((F.col("plan_type") == "MAPD") & (F.col("segment") == "tenured"), F.lit("MAPD Tenured"))
        .otherwise(F.lit("Commercial"))
    )
    .select(
        "member_id",
        "plan_type",
        "tenure_months",
        "segment",
        "language_preference",
        "enrollment_date",
        "member_category",
        "ingestion_timestamp",
        "processing_timestamp"
    )
)

# Write to silver table
(member_profiles_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.member_profiles_enriched")
)

print(f"Created silver.member_profiles_enriched: {member_profiles_silver.count():,} records")
member_profiles_silver.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Digital Journeys

# COMMAND ----------

# Clean digital journeys
digital_journeys_silver = (digital_journeys_bronze
    .filter(F.col("member_id").isNotNull())
    .filter(F.col("session_id").isNotNull())
    .filter(F.col("timestamp").isNotNull())
    .dropDuplicates(["session_id", "timestamp", "action"])
    .withColumn("processing_timestamp", F.current_timestamp())
    # Standardize device types
    .withColumn("device_type",
        F.when(F.lower(F.col("device_type")).isin(["mobile", "phone", "smartphone"]), F.lit("mobile"))
        .when(F.lower(F.col("device_type")).isin(["desktop", "computer", "pc"]), F.lit("desktop"))
        .when(F.lower(F.col("device_type")).isin(["tablet", "ipad"]), F.lit("tablet"))
        .otherwise(F.col("device_type"))
    )
    .select(
        "member_id",
        "session_id",
        "timestamp",
        "journey_type",
        "action",
        "device_type",
        "error_code",
        "error_message",
        "ingestion_timestamp",
        "processing_timestamp"
    )
)

# Write to silver table
(digital_journeys_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.digital_journeys_cleaned")
)

print(f"Created silver.digital_journeys_cleaned: {digital_journeys_silver.count():,} records")
digital_journeys_silver.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Call Logs

# COMMAND ----------

# Clean call logs
call_logs_silver = (call_logs_bronze
    .filter(F.col("call_id").isNotNull())
    .filter(F.col("member_id").isNotNull())
    .filter(F.col("call_timestamp").isNotNull())
    .dropDuplicates(["call_id"])
    .withColumn("processing_timestamp", F.current_timestamp())
    # Ensure sentiment is within valid range
    .withColumn("sentiment_score",
        F.when(F.col("sentiment_score") > 1.0, F.lit(1.0))
        .when(F.col("sentiment_score") < -1.0, F.lit(-1.0))
        .otherwise(F.col("sentiment_score"))
    )
    # Ensure handle_time is positive
    .withColumn("handle_time_seconds",
        F.when(F.col("handle_time_seconds") < 0, F.lit(0))
        .otherwise(F.col("handle_time_seconds"))
    )
    .select(
        "call_id",
        "member_id",
        "call_timestamp",
        "ivr_path",
        "handle_time_seconds",
        "agent_id",
        "call_reason",
        "sentiment_score",
        "resolution_status",
        "ingestion_timestamp",
        "processing_timestamp"
    )
)

# Write to silver table
(call_logs_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.call_logs_cleaned")
)

print(f"Created silver.call_logs_cleaned: {call_logs_silver.count():,} records")
call_logs_silver.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Digital-to-Call Leakage
# MAGIC 
# MAGIC **Leakage Definition**: A digital journey that ends in abandon/error followed by a call within 30 minutes

# COMMAND ----------

# Get digital abandons and errors (potential leakage events)
digital_leakage_events = (digital_journeys_silver
    .filter(F.col("action").isin(["abandon", "error"]))
    .select(
        F.col("member_id").alias("digital_member_id"),
        F.col("session_id").alias("digital_session_id"),
        F.col("timestamp").alias("digital_timestamp"),
        F.col("journey_type").alias("digital_journey_type"),
        F.col("device_type").alias("digital_device_type"),
        F.col("error_code").alias("digital_error_code"),
        F.col("error_message").alias("digital_error_message")
    )
)

# Get all calls
calls = (call_logs_silver
    .select(
        F.col("call_id"),
        F.col("member_id").alias("call_member_id"),
        F.col("call_timestamp"),
        F.col("call_reason"),
        F.col("sentiment_score"),
        F.col("handle_time_seconds"),
        F.col("ivr_path")
    )
)

# Join digital events with calls on member_id and time window
# A call is considered leakage if it occurs within LEAKAGE_WINDOW_MINUTES after digital abandon/error
digital_call_leakage = (digital_leakage_events
    .join(
        calls,
        (digital_leakage_events.digital_member_id == calls.call_member_id) &
        (calls.call_timestamp >= digital_leakage_events.digital_timestamp) &
        (calls.call_timestamp <= digital_leakage_events.digital_timestamp + F.expr(f"INTERVAL {LEAKAGE_WINDOW_MINUTES} MINUTES")),
        "inner"
    )
    .withColumn("leakage_id", F.concat(F.col("digital_session_id"), F.lit("_"), F.col("call_id")))
    .withColumn("is_leakage_call", F.lit(True))
    .withColumn("leakage_type", F.col("digital_journey_type"))
    .withColumn("time_to_call_minutes",
        F.round((F.unix_timestamp("call_timestamp") - F.unix_timestamp("digital_timestamp")) / 60, 0).cast(IntegerType())
    )
)

# Enrich with member profile data
digital_call_leakage_enriched = (digital_call_leakage
    .join(
        member_profiles_silver.select(
            "member_id",
            "member_category",
            "plan_type",
            "segment",
            "language_preference"
        ),
        digital_call_leakage.digital_member_id == member_profiles_silver.member_id,
        "left"
    )
)

# Detect repeat calls (same member calling within 7 days)
window_spec = Window.partitionBy("digital_member_id").orderBy("call_timestamp")
digital_call_leakage_with_repeat = (digital_call_leakage_enriched
    .withColumn("prev_call_timestamp", F.lag("call_timestamp").over(window_spec))
    .withColumn("days_since_last_call",
        F.when(F.col("prev_call_timestamp").isNotNull(),
            F.datediff(F.col("call_timestamp"), F.col("prev_call_timestamp"))
        ).otherwise(999)
    )
    .withColumn("is_repeat_call",
        F.when(F.col("days_since_last_call") <= 7, F.lit(True))
        .otherwise(F.lit(False))
    )
    .withColumn("processing_timestamp", F.current_timestamp())
)

# Final leakage table
digital_call_leakage_final = (digital_call_leakage_with_repeat
    .select(
        "leakage_id",
        F.col("digital_member_id").alias("member_id"),
        "digital_session_id",
        "digital_timestamp",
        "call_id",
        "call_timestamp",
        "is_leakage_call",
        "leakage_type",
        "time_to_call_minutes",
        "is_repeat_call",
        "member_category",
        "plan_type",
        "segment",
        "language_preference",
        "sentiment_score",
        "handle_time_seconds",
        F.col("digital_device_type").alias("device_type"),
        F.col("digital_error_code").alias("error_code"),
        "processing_timestamp"
    )
)

# Write to silver table
(digital_call_leakage_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.digital_call_leakage")
)

print(f"Created silver.digital_call_leakage: {digital_call_leakage_final.count():,} records")
digital_call_leakage_final.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leakage Analysis

# COMMAND ----------

print("\n" + "=" * 80)
print("LEAKAGE ANALYSIS")
print("=" * 80)

# Overall leakage summary
leakage_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.digital_call_leakage")

total_leakage = leakage_df.count()
print(f"\nTotal Leakage Events: {total_leakage:,}")
print(f"Cost at $8/call: ${total_leakage * 8:,}")

# Leakage by journey type
print("\nLeakage by Journey Type:")
leakage_df.groupBy("leakage_type").agg(
    F.count("*").alias("calls"),
    F.round(F.avg("sentiment_score"), 2).alias("avg_sentiment"),
    F.round(F.avg("time_to_call_minutes"), 0).alias("avg_time_to_call_min")
).orderBy(F.desc("calls")).show()

# Leakage by member category
print("\nLeakage by Member Category:")
leakage_df.groupBy("member_category").agg(
    F.count("*").alias("calls"),
    F.round(F.avg("sentiment_score"), 2).alias("avg_sentiment"),
    F.round(F.sum(F.when(F.col("is_repeat_call"), 1).otherwise(0)) / F.count("*") * 100, 1).alias("repeat_call_pct")
).orderBy(F.desc("calls")).show()

# Leakage by device type
print("\nLeakage by Device Type:")
leakage_df.groupBy("device_type").agg(
    F.count("*").alias("calls"),
    F.round(F.count("*") / total_leakage * 100, 1).alias("pct_of_total")
).orderBy(F.desc("calls")).show()

# Top error codes driving leakage
print("\nTop Error Codes Driving Leakage:")
leakage_df.filter(F.col("error_code").isNotNull()).groupBy("error_code", "leakage_type").agg(
    F.count("*").alias("calls")
).orderBy(F.desc("calls")).show(10)

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Queries

# COMMAND ----------

print("\nSilver Layer Tables Created:")
print("=" * 80)

tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SILVER_SCHEMA}").collect()
for table in tables:
    table_name = table.tableName
    count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table_name}").count()
    print(f"  ✓ {table_name}: {count:,} records")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Queries for Validation

# COMMAND ----------

# Query: Top 5 members with most leakage calls
print("\nTop 5 Members with Most Leakage Calls:")
spark.sql(f"""
    SELECT 
        member_id,
        member_category,
        COUNT(*) as total_calls,
        SUM(CASE WHEN is_repeat_call THEN 1 ELSE 0 END) as repeat_calls,
        ROUND(AVG(sentiment_score), 2) as avg_sentiment
    FROM {CATALOG}.{SILVER_SCHEMA}.digital_call_leakage
    GROUP BY member_id, member_category
    ORDER BY total_calls DESC
    LIMIT 5
""").show()

# Query: Leakage trend by hour of day
print("\nLeakage Calls by Hour of Day:")
spark.sql(f"""
    SELECT 
        HOUR(call_timestamp) as hour_of_day,
        COUNT(*) as leakage_calls,
        ROUND(AVG(time_to_call_minutes), 1) as avg_time_to_call
    FROM {CATALOG}.{SILVER_SCHEMA}.digital_call_leakage
    GROUP BY hour_of_day
    ORDER BY hour_of_day
""").show(24)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 80)
print("SILVER TRANSFORMATION SUMMARY")
print("=" * 80)

print(f"\nCatalog: {CATALOG}")
print(f"Schema: {SILVER_SCHEMA}")
print(f"\nTables Created:")
print(f"  ✓ member_profiles_enriched")
print(f"  ✓ digital_journeys_cleaned")
print(f"  ✓ call_logs_cleaned")
print(f"  ✓ digital_call_leakage (main analytical table)")

print(f"\nKey Metrics:")
print(f"  - Total leakage events identified: {total_leakage:,}")
print(f"  - Leakage window used: {LEAKAGE_WINDOW_MINUTES} minutes")
print(f"  - Daily cost (at $8/call): ${total_leakage * 8:,}")

print(f"\nStatus: ✓ Ready for Gold layer aggregations")

print("=" * 80)
