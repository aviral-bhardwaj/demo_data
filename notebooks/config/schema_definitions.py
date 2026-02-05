# Databricks notebook source
"""
Schema Definitions for Digital-to-Call Leakage Detection
Medallion Architecture: Bronze, Silver, Gold Layer Schemas
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType, DateType
)

# COMMAND ----------

# Bronze Layer Schemas (Raw data ingestion)

DIGITAL_JOURNEYS_BRONZE_SCHEMA = StructType([
    StructField("member_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("journey_type", StringType(), True),  # billing/id_card/pharmacy
    StructField("action", StringType(), True),  # login, page_view, abandon, error
    StructField("device_type", StringType(), True),  # mobile, desktop, tablet
    StructField("error_code", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("source_file", StringType(), True)
])

CALL_LOGS_BRONZE_SCHEMA = StructType([
    StructField("call_id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("call_timestamp", TimestampType(), True),
    StructField("ivr_path", StringType(), True),
    StructField("handle_time_seconds", IntegerType(), True),
    StructField("agent_id", StringType(), True),
    StructField("call_reason", StringType(), True),
    StructField("sentiment_score", DoubleType(), True),  # -1 to 1
    StructField("resolution_status", StringType(), True),  # resolved, escalated, pending
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("source_file", StringType(), True)
])

MEMBER_PROFILES_BRONZE_SCHEMA = StructType([
    StructField("member_id", StringType(), True),
    StructField("plan_type", StringType(), True),  # MAPD, Commercial
    StructField("tenure_months", IntegerType(), True),
    StructField("segment", StringType(), True),  # new, tenured
    StructField("language_preference", StringType(), True),  # English, Spanish, Other
    StructField("enrollment_date", DateType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("source_file", StringType(), True)
])

# COMMAND ----------

# Silver Layer Schemas (Cleaned and enriched)

DIGITAL_JOURNEYS_SILVER_SCHEMA = StructType([
    StructField("member_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("journey_type", StringType(), False),
    StructField("action", StringType(), False),
    StructField("device_type", StringType(), False),
    StructField("error_code", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("processing_timestamp", TimestampType(), False)
])

CALL_LOGS_SILVER_SCHEMA = StructType([
    StructField("call_id", StringType(), False),
    StructField("member_id", StringType(), False),
    StructField("call_timestamp", TimestampType(), False),
    StructField("ivr_path", StringType(), True),
    StructField("handle_time_seconds", IntegerType(), False),
    StructField("agent_id", StringType(), True),
    StructField("call_reason", StringType(), False),
    StructField("sentiment_score", DoubleType(), False),
    StructField("resolution_status", StringType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("processing_timestamp", TimestampType(), False)
])

MEMBER_PROFILES_SILVER_SCHEMA = StructType([
    StructField("member_id", StringType(), False),
    StructField("plan_type", StringType(), False),
    StructField("tenure_months", IntegerType(), False),
    StructField("segment", StringType(), False),
    StructField("language_preference", StringType(), False),
    StructField("enrollment_date", DateType(), False),
    StructField("member_category", StringType(), False),  # Derived: MAPD New, MAPD Tenured, Commercial
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("processing_timestamp", TimestampType(), False)
])

DIGITAL_CALL_LEAKAGE_SILVER_SCHEMA = StructType([
    StructField("leakage_id", StringType(), False),
    StructField("member_id", StringType(), False),
    StructField("digital_session_id", StringType(), False),
    StructField("digital_timestamp", TimestampType(), False),
    StructField("call_id", StringType(), False),
    StructField("call_timestamp", TimestampType(), False),
    StructField("is_leakage_call", BooleanType(), False),
    StructField("leakage_type", StringType(), True),  # billing, id_card, pharmacy
    StructField("time_to_call_minutes", IntegerType(), True),
    StructField("is_repeat_call", BooleanType(), False),
    StructField("member_category", StringType(), True),
    StructField("plan_type", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("language_preference", StringType(), True),
    StructField("sentiment_score", DoubleType(), True),
    StructField("handle_time_seconds", IntegerType(), True),
    StructField("device_type", StringType(), True),
    StructField("error_code", StringType(), True),
    StructField("processing_timestamp", TimestampType(), False)
])

# COMMAND ----------

# Gold Layer Schemas (Aggregated for dashboards)

DAILY_LEAKAGE_SUMMARY_SCHEMA = StructType([
    StructField("summary_date", DateType(), False),
    StructField("journey_type", StringType(), False),
    StructField("daily_calls", IntegerType(), False),
    StructField("daily_cost", DoubleType(), False),
    StructField("segment", StringType(), False),
    StructField("trend_pct", DoubleType(), True),
    StructField("processing_timestamp", TimestampType(), False)
])

SEGMENT_DRILLDOWN_SCHEMA = StructType([
    StructField("segment", StringType(), False),
    StructField("journey_type", StringType(), False),
    StructField("daily_calls", IntegerType(), False),
    StructField("daily_cost", DoubleType(), False),
    StructField("repeat_call_pct", DoubleType(), False),
    StructField("avg_sentiment", DoubleType(), False),
    StructField("processing_timestamp", TimestampType(), False)
])

JOURNEY_TIMELINE_SCHEMA = StructType([
    StructField("member_id", StringType(), False),
    StructField("event_sequence", IntegerType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("event_type", StringType(), False),  # digital, call
    StructField("event_description", StringType(), False),
    StructField("sentiment_score", DoubleType(), True),
    StructField("processing_timestamp", TimestampType(), False)
])

ROOT_CAUSE_ANALYSIS_SCHEMA = StructType([
    StructField("journey_type", StringType(), False),
    StructField("error_code", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("device_type", StringType(), False),
    StructField("leakage_count", IntegerType(), False),
    StructField("leakage_pct", DoubleType(), False),
    StructField("trend_date", DateType(), False),
    StructField("processing_timestamp", TimestampType(), False)
])

SAVINGS_MODEL_SCHEMA = StructType([
    StructField("journey_type", StringType(), False),
    StructField("avoidable_calls_per_day", IntegerType(), False),
    StructField("cost_per_call", DoubleType(), False),
    StructField("annualized_cost", DoubleType(), False),
    StructField("fix_rate_10_pct_savings", DoubleType(), False),
    StructField("fix_rate_25_pct_savings", DoubleType(), False),
    StructField("fix_rate_50_pct_savings", DoubleType(), False),
    StructField("estimated_nps_lift", IntegerType(), True),
    StructField("processing_timestamp", TimestampType(), False)
])

ACTION_RECOMMENDATIONS_SCHEMA = StructType([
    StructField("action_id", StringType(), False),
    StructField("journey_type", StringType(), False),
    StructField("action_title", StringType(), False),
    StructField("action_description", StringType(), False),
    StructField("owner", StringType(), False),  # Digital, Comms, Contact Center
    StructField("estimated_savings_annual", DoubleType(), False),
    StructField("effort_estimate", StringType(), False),  # days, weeks, sprints
    StructField("priority_rank", IntegerType(), False),
    StructField("cx_impact_score", IntegerType(), True),  # 1-10
    StructField("processing_timestamp", TimestampType(), False)
])

# COMMAND ----------

# Constants
COST_PER_CALL = 8.0
LEAKAGE_WINDOW_MINUTES = 30

# Journey Types
JOURNEY_BILLING = "billing"
JOURNEY_ID_CARD = "id_card"
JOURNEY_PHARMACY = "pharmacy"

# Actions
ACTION_LOGIN = "login"
ACTION_PAGE_VIEW = "page_view"
ACTION_ABANDON = "abandon"
ACTION_ERROR = "error"

# Device Types
DEVICE_MOBILE = "mobile"
DEVICE_DESKTOP = "desktop"
DEVICE_TABLET = "tablet"

# Segments
SEGMENT_NEW = "new"
SEGMENT_TENURED = "tenured"

# Plan Types
PLAN_MAPD = "MAPD"
PLAN_COMMERCIAL = "Commercial"

# Languages
LANG_ENGLISH = "English"
LANG_SPANISH = "Spanish"
LANG_OTHER = "Other"
