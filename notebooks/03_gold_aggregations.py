# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Layer Aggregations
# MAGIC 
# MAGIC **Purpose**: Create aggregated tables for the Digital-to-Call Leakage Command Center Dashboard
# MAGIC 
# MAGIC **Gold Layer Philosophy**:
# MAGIC - Business-level aggregations
# MAGIC - Optimized for dashboard queries
# MAGIC - Pre-calculated metrics
# MAGIC - Denormalized for performance
# MAGIC 
# MAGIC **Tables Created**:
# MAGIC - `gold.daily_leakage_summary` - Executive overview metrics
# MAGIC - `gold.segment_drilldown` - Segment-level analysis
# MAGIC - `gold.journey_timeline` - Member journey playback
# MAGIC - `gold.root_cause_analysis` - Error and device breakdown
# MAGIC - `gold.savings_model` - Financial projections
# MAGIC - `gold.action_recommendations` - Prioritized actions

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
dbutils.widgets.text("silver_schema", "silver", "Silver Schema Name")
dbutils.widgets.text("gold_schema", "gold", "Gold Schema Name")
dbutils.widgets.text("cost_per_call", "8.0", "Cost Per Call ($)")

# Get widget values
CATALOG = dbutils.widgets.get("catalog_name")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")
COST_PER_CALL = float(dbutils.widgets.get("cost_per_call"))

print(f"Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Silver Schema: {SILVER_SCHEMA}")
print(f"  Gold Schema: {GOLD_SCHEMA}")
print(f"  Cost Per Call: ${COST_PER_CALL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold Schema

# COMMAND ----------

# Create gold schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")

print(f"Using catalog: {CATALOG}")
print(f"Gold schema created/verified: {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Tables

# COMMAND ----------

# Load silver tables
digital_journeys_silver = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.digital_journeys_cleaned")
call_logs_silver = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.call_logs_cleaned")
member_profiles_silver = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.member_profiles_enriched")
leakage_silver = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.digital_call_leakage")

print(f"Loaded silver tables:")
print(f"  - digital_journeys_cleaned: {digital_journeys_silver.count():,} records")
print(f"  - call_logs_cleaned: {call_logs_silver.count():,} records")
print(f"  - member_profiles_enriched: {member_profiles_silver.count():,} records")
print(f"  - digital_call_leakage: {leakage_silver.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Daily Leakage Summary
# MAGIC 
# MAGIC Executive overview showing daily leakage by journey type, segment, and trend

# COMMAND ----------

# Calculate daily leakage summary with trends
daily_leakage_summary = (leakage_silver
    .withColumn("summary_date", F.to_date("call_timestamp"))
    .groupBy("summary_date", "leakage_type", "member_category")
    .agg(
        F.count("*").alias("daily_calls"),
        (F.count("*") * F.lit(COST_PER_CALL)).alias("daily_cost")
    )
    .withColumn("segment", F.col("member_category"))
)

# Calculate trends (comparing to previous period - simulated for demo)
# In production, this would compare to historical data
daily_leakage_with_trend = (daily_leakage_summary
    .withColumn("trend_pct",
        F.when(F.col("leakage_type") == "billing", 
            F.when(F.col("segment") == "MAPD New", F.lit(14.0))
            .otherwise(F.lit(5.0))
        )
        .when(F.col("leakage_type") == "id_card",
            F.when(F.col("segment").contains("Spanish"), F.lit(8.0))  # Simplified check
            .otherwise(F.lit(3.0))
        )
        .when(F.col("leakage_type") == "pharmacy", F.lit(3.0))
        .otherwise(F.lit(0.0))
    )
    .withColumn("processing_timestamp", F.current_timestamp())
)

# Write to gold table
(daily_leakage_with_trend
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.daily_leakage_summary")
)

print(f"Created gold.daily_leakage_summary: {daily_leakage_with_trend.count():,} records")
daily_leakage_with_trend.orderBy(F.desc("daily_cost")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Segment Drilldown
# MAGIC 
# MAGIC Detailed breakdown by member segment with key metrics

# COMMAND ----------

# Segment drilldown analysis
segment_drilldown = (leakage_silver
    .groupBy("member_category", "leakage_type")
    .agg(
        F.count("*").alias("daily_calls"),
        (F.count("*") * F.lit(COST_PER_CALL)).alias("daily_cost"),
        F.round(
            (F.sum(F.when(F.col("is_repeat_call"), 1).otherwise(0)) / F.count("*")) * 100, 
            1
        ).alias("repeat_call_pct"),
        F.round(F.avg("sentiment_score"), 2).alias("avg_sentiment")
    )
    .withColumn("segment", F.col("member_category"))
    .withColumn("processing_timestamp", F.current_timestamp())
    .select(
        "segment",
        "leakage_type",
        "daily_calls",
        "daily_cost",
        "repeat_call_pct",
        "avg_sentiment",
        "processing_timestamp"
    )
)

# Write to gold table
(segment_drilldown
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.segment_drilldown")
)

print(f"Created gold.segment_drilldown: {segment_drilldown.count():,} records")
segment_drilldown.orderBy(F.desc("daily_cost")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Journey Timeline
# MAGIC 
# MAGIC Individual member journey playback for detailed analysis

# COMMAND ----------

# Create journey timeline by combining digital and call events
# Get sample members with leakage for timeline
sample_members = (leakage_silver
    .select("member_id")
    .distinct()
    .limit(1000)  # Limit to keep table size manageable
)

# Digital events for sample members
digital_events = (digital_journeys_silver
    .join(sample_members, "member_id")
    .select(
        F.col("member_id"),
        F.col("timestamp").alias("event_timestamp"),
        F.lit("digital").alias("event_type"),
        F.concat(
            F.lit("Digital: "),
            F.col("journey_type"),
            F.lit(" - "),
            F.col("action"),
            F.when(F.col("error_message").isNotNull(), 
                F.concat(F.lit(" (Error: "), F.col("error_message"), F.lit(")"))
            ).otherwise(F.lit(""))
        ).alias("event_description"),
        F.lit(None).cast(DoubleType()).alias("sentiment_score")
    )
)

# Call events for sample members
call_events = (call_logs_silver
    .join(sample_members, "member_id")
    .select(
        F.col("member_id"),
        F.col("call_timestamp").alias("event_timestamp"),
        F.lit("call").alias("event_type"),
        F.concat(
            F.lit("Call: "),
            F.col("call_reason"),
            F.lit(" (Handle time: "),
            F.round(F.col("handle_time_seconds") / 60, 1),
            F.lit(" min)")
        ).alias("event_description"),
        F.col("sentiment_score")
    )
)

# Union and add sequence numbers
journey_timeline = (digital_events
    .union(call_events)
    .withColumn(
        "event_sequence",
        F.row_number().over(
            Window.partitionBy("member_id").orderBy("event_timestamp")
        )
    )
    .withColumn("processing_timestamp", F.current_timestamp())
    .select(
        "member_id",
        "event_sequence",
        "event_timestamp",
        "event_type",
        "event_description",
        "sentiment_score",
        "processing_timestamp"
    )
)

# Write to gold table
(journey_timeline
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.journey_timeline")
)

print(f"Created gold.journey_timeline: {journey_timeline.count():,} records")
print("\nSample journey for one member:")
journey_timeline.filter(F.col("member_id") == journey_timeline.select("member_id").first()[0]).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Root Cause Analysis
# MAGIC 
# MAGIC Error codes, device breakdown, and trends

# COMMAND ----------

# Root cause analysis by error code and device
root_cause_analysis = (leakage_silver
    .withColumn("trend_date", F.to_date("call_timestamp"))
    .groupBy("leakage_type", "error_code", "device_type", "trend_date")
    .agg(
        F.count("*").alias("leakage_count")
    )
)

# Calculate percentages within journey type
window_spec = Window.partitionBy("leakage_type", "trend_date")
root_cause_with_pct = (root_cause_analysis
    .withColumn(
        "leakage_pct",
        F.round((F.col("leakage_count") / F.sum("leakage_count").over(window_spec)) * 100, 1)
    )
    .withColumn("processing_timestamp", F.current_timestamp())
)

# Add error messages by joining back to leakage table
root_cause_with_messages = (root_cause_with_pct
    .join(
        leakage_silver.select("error_code", "leakage_type", 
            F.first("error_code").over(Window.partitionBy("error_code", "leakage_type")).alias("error_message")
        ).distinct(),
        ["error_code", "leakage_type"],
        "left"
    )
    .select(
        "leakage_type",
        "error_code",
        "error_message",
        "device_type",
        "leakage_count",
        "leakage_pct",
        "trend_date",
        "processing_timestamp"
    )
)

# Write to gold table
(root_cause_with_messages
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.root_cause_analysis")
)

print(f"Created gold.root_cause_analysis: {root_cause_with_messages.count():,} records")
print("\nTop root causes:")
root_cause_with_messages.orderBy(F.desc("leakage_count")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Savings Model
# MAGIC 
# MAGIC Financial projections at different fix rates

# COMMAND ----------

# Calculate savings model by journey type
savings_base = (leakage_silver
    .groupBy("leakage_type")
    .agg(
        F.count("*").alias("avoidable_calls_per_day")
    )
    .withColumn("cost_per_call", F.lit(COST_PER_CALL))
    .withColumn("annualized_cost", F.col("avoidable_calls_per_day") * F.col("cost_per_call") * 365)
)

# Calculate savings at different fix rates
savings_model = (savings_base
    .withColumn("fix_rate_10_pct_savings", F.round(F.col("annualized_cost") * 0.10, 0))
    .withColumn("fix_rate_25_pct_savings", F.round(F.col("annualized_cost") * 0.25, 0))
    .withColumn("fix_rate_50_pct_savings", F.round(F.col("annualized_cost") * 0.50, 0))
    .withColumn("estimated_nps_lift",
        F.when(F.col("leakage_type") == "billing", F.lit(10))
        .when(F.col("leakage_type") == "id_card", F.lit(8))
        .when(F.col("leakage_type") == "pharmacy", F.lit(6))
        .otherwise(F.lit(5))
    )
    .withColumn("processing_timestamp", F.current_timestamp())
)

# Write to gold table
(savings_model
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.savings_model")
)

print(f"Created gold.savings_model: {savings_model.count():,} records")
savings_model.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Action Recommendations
# MAGIC 
# MAGIC Prioritized recommendations with estimated savings

# COMMAND ----------

# Define action recommendations based on analysis
# In production, this could be ML-driven or rules-based
actions_data = [
    # Billing actions
    ("ACT-001", "billing", "Update MAPD Billing Eligibility Message",
     "Change the MAPD billing eligibility message on mobile and add clear next steps. Focus on the 'Your plan is not eligible' error.",
     "Digital UX", 1000000.0, "1 sprint", 1, 10),
    ("ACT-002", "billing", "Proactive IVR Messaging for MAPD",
     "Add proactive message in IVR: 'If you're seeing a billing message in the app, here's what it means...'",
     "Contact Center", 300000.0, "1 week", 2, 8),
    ("ACT-003", "billing", "Simplified Agent Script for MAPD",
     "Create simplified explanation macro for MAPD billing calls to reduce handle time.",
     "Contact Center", 160000.0, "3 days", 3, 7),
    
    # ID Card actions
    ("ACT-004", "id_card", "Spanish Language ID Card Download Fix",
     "Fix ID card download functionality for Spanish language users. Add better error handling.",
     "Digital UX", 600000.0, "2 sprints", 4, 9),
    ("ACT-005", "id_card", "ID Card Self-Service Enhancement",
     "Add ability to email ID card directly from app when download fails.",
     "Digital UX", 400000.0, "1 sprint", 5, 8),
    
    # Pharmacy actions
    ("ACT-006", "pharmacy", "Pharmacy Refill Error Message Clarity",
     "Improve error messages for pharmacy refills to explain prior authorization and network issues.",
     "Digital UX", 500000.0, "1 sprint", 6, 7),
    ("ACT-007", "pharmacy", "Pharmacy IVR Integration",
     "Integrate pharmacy status check in IVR before routing to agent.",
     "Contact Center", 350000.0, "2 weeks", 7, 6)
]

actions_schema = StructType([
    StructField("action_id", StringType(), False),
    StructField("journey_type", StringType(), False),
    StructField("action_title", StringType(), False),
    StructField("action_description", StringType(), False),
    StructField("owner", StringType(), False),
    StructField("estimated_savings_annual", DoubleType(), False),
    StructField("effort_estimate", StringType(), False),
    StructField("priority_rank", IntegerType(), False),
    StructField("cx_impact_score", IntegerType(), False)
])

action_recommendations = (spark.createDataFrame(actions_data, schema=actions_schema)
    .withColumn("processing_timestamp", F.current_timestamp())
)

# Write to gold table
(action_recommendations
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.action_recommendations")
)

print(f"Created gold.action_recommendations: {action_recommendations.count():,} records")
action_recommendations.orderBy("priority_rank").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Summary Metrics

# COMMAND ----------

print("\n" + "=" * 80)
print("COMMAND CENTER DASHBOARD - KEY METRICS")
print("=" * 80)

# Executive Overview
print("\n1. EXECUTIVE OVERVIEW")
print("-" * 80)

exec_summary = spark.sql(f"""
    SELECT 
        leakage_type as journey,
        SUM(daily_calls) as daily_calls,
        CONCAT('$', FORMAT_NUMBER(SUM(daily_cost), 0)) as daily_cost,
        segment,
        CONCAT('+', CAST(MAX(trend_pct) AS INT), '%') as trend
    FROM {CATALOG}.{GOLD_SCHEMA}.daily_leakage_summary
    GROUP BY leakage_type, segment
    ORDER BY SUM(daily_calls) DESC
    LIMIT 5
""")
exec_summary.show(truncate=False)

# Segment Drill-Down
print("\n2. SEGMENT DRILL-DOWN (Billing)")
print("-" * 80)

segment_drill = spark.sql(f"""
    SELECT 
        segment,
        daily_calls,
        CONCAT('$', FORMAT_NUMBER(daily_cost, 0)) as daily_cost,
        CONCAT(CAST(repeat_call_pct AS INT), '%') as repeat_call_pct,
        avg_sentiment
    FROM {CATALOG}.{GOLD_SCHEMA}.segment_drilldown
    WHERE leakage_type = 'billing'
    ORDER BY daily_calls DESC
""")
segment_drill.show(truncate=False)

# Savings Model
print("\n3. FINANCIAL SAVINGS MODEL")
print("-" * 80)

savings = spark.sql(f"""
    SELECT 
        leakage_type as journey,
        avoidable_calls_per_day,
        CONCAT('$', FORMAT_NUMBER(annualized_cost, 0)) as annualized_cost,
        CONCAT('$', FORMAT_NUMBER(fix_rate_10_pct_savings, 0)) as savings_at_10pct,
        CONCAT('$', FORMAT_NUMBER(fix_rate_50_pct_savings, 0)) as savings_at_50pct,
        CONCAT('+', estimated_nps_lift) as nps_lift
    FROM {CATALOG}.{GOLD_SCHEMA}.savings_model
    ORDER BY annualized_cost DESC
""")
savings.show(truncate=False)

# Top Root Causes
print("\n4. ROOT CAUSE ANALYSIS")
print("-" * 80)

root_causes = spark.sql(f"""
    SELECT 
        leakage_type,
        error_code,
        device_type,
        SUM(leakage_count) as total_leakage,
        CONCAT(CAST(AVG(leakage_pct) AS INT), '%') as pct_of_type
    FROM {CATALOG}.{GOLD_SCHEMA}.root_cause_analysis
    WHERE error_code IS NOT NULL
    GROUP BY leakage_type, error_code, device_type
    ORDER BY total_leakage DESC
    LIMIT 10
""")
root_causes.show(truncate=False)

# Action Recommendations
print("\n5. NEXT-BEST ACTIONS")
print("-" * 80)

actions = spark.sql(f"""
    SELECT 
        priority_rank,
        action_title,
        owner,
        CONCAT('$', FORMAT_NUMBER(estimated_savings_annual/1000000, 1), 'M') as est_savings,
        effort_estimate,
        cx_impact_score
    FROM {CATALOG}.{GOLD_SCHEMA}.action_recommendations
    ORDER BY priority_rank
    LIMIT 5
""")
actions.show(truncate=False)

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Validation

# COMMAND ----------

print("\nGold Layer Tables Created:")
print("=" * 80)

tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{GOLD_SCHEMA}").collect()
for table in tables:
    table_name = table.tableName
    count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{table_name}").count()
    print(f"  ✓ {table_name}: {count:,} records")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Dashboard Queries

# COMMAND ----------

# Query 1: What's breaking right now?
print("\nQuery: What's breaking right now?")
print("-" * 80)
spark.sql(f"""
    SELECT 
        leakage_type as 'Journey Type',
        SUM(daily_calls) as 'Daily Calls',
        CONCAT('$', FORMAT_NUMBER(SUM(daily_cost), 0)) as 'Daily Cost',
        MAX(segment) as 'Primary Segment',
        CONCAT('+', CAST(MAX(trend_pct) AS INT), '%') as 'Trend'
    FROM {CATALOG}.{GOLD_SCHEMA}.daily_leakage_summary
    GROUP BY leakage_type
    ORDER BY SUM(daily_calls) DESC
""").show(truncate=False)

# Query 2: If we fix billing, what's the upside?
print("\nQuery: If we fix billing, what's the upside?")
print("-" * 80)
spark.sql(f"""
    SELECT 
        'Billing Fix' as scenario,
        avoidable_calls_per_day as 'Avoidable Calls/Day',
        CONCAT('$', FORMAT_NUMBER(annualized_cost, 0)) as 'Current Annual Cost',
        CONCAT('$', FORMAT_NUMBER(fix_rate_10_pct_savings, 0)) as '10% Fix Savings',
        CONCAT('$', FORMAT_NUMBER(fix_rate_25_pct_savings, 0)) as '25% Fix Savings',
        CONCAT('$', FORMAT_NUMBER(fix_rate_50_pct_savings, 0)) as '50% Fix Savings'
    FROM {CATALOG}.{GOLD_SCHEMA}.savings_model
    WHERE leakage_type = 'billing'
""").show(truncate=False)

# Query 3: What should my teams do next?
print("\nQuery: What should my teams do next?")
print("-" * 80)
spark.sql(f"""
    SELECT 
        priority_rank as 'Priority',
        action_title as 'Action',
        owner as 'Owner',
        CONCAT('$', FORMAT_NUMBER(estimated_savings_annual/1000000, 1), 'M/yr') as 'Est. Savings',
        effort_estimate as 'Effort',
        cx_impact_score as 'CX Impact (1-10)'
    FROM {CATALOG}.{GOLD_SCHEMA}.action_recommendations
    ORDER BY priority_rank
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 80)
print("GOLD AGGREGATION SUMMARY")
print("=" * 80)

print(f"\nCatalog: {CATALOG}")
print(f"Schema: {GOLD_SCHEMA}")
print(f"\nTables Created:")
print(f"  ✓ daily_leakage_summary - Executive overview metrics")
print(f"  ✓ segment_drilldown - Segment-level analysis")
print(f"  ✓ journey_timeline - Member journey playback")
print(f"  ✓ root_cause_analysis - Error and device breakdown")
print(f"  ✓ savings_model - Financial projections")
print(f"  ✓ action_recommendations - Prioritized actions")

print(f"\nKey Insights:")
total_calls = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.daily_leakage_summary").agg(F.sum("daily_calls")).collect()[0][0]
total_cost = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.daily_leakage_summary").agg(F.sum("daily_cost")).collect()[0][0]
print(f"  - Total daily leakage: {total_calls:,.0f} calls")
print(f"  - Total daily cost: ${total_cost:,.0f}")
print(f"  - Annualized cost: ${total_cost * 365:,.0f}")

print(f"\nDashboard Views Supported:")
print(f"  ✓ Executive Overview")
print(f"  ✓ Segment Drill-Down")
print(f"  ✓ Journey Playback Timeline")
print(f"  ✓ Financial Savings Model")
print(f"  ✓ Root Cause Analysis")
print(f"  ✓ Next-Best Action Recommendations")

print(f"\nStatus: ✓ Ready for dashboard visualization and pipeline orchestration")

print("=" * 80)
