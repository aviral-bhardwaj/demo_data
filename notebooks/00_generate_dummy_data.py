# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Generate Dummy Data
# MAGIC
# MAGIC **Purpose**: Generate synthetic data for Digital-to-Call Leakage Detection use case
# MAGIC
# MAGIC **Data Generated**:
# MAGIC - Digital Journeys (~50,000 records)
# MAGIC - Call Logs (~12,000 records)
# MAGIC - Member Profiles (~10,000 records)
# MAGIC
# MAGIC **Key Patterns**:
# MAGIC - 5,000 daily billing abandon → call (MAPD New, +14% trend)
# MAGIC - 3,000 daily ID card fail → call (Spanish, +8% trend)
# MAGIC - 4,000 daily pharmacy → call (Mixed, +3% trend)
# MAGIC - Cost per call: $8
# MAGIC - MAPD New: 28% repeat calls, -0.65 sentiment

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Import required libraries
from faker import Faker
import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for configuration
dbutils.widgets.text("output_path", "/Volumes/insurance_final_with_ai/default/data/inputpath/", "Output Path")
dbutils.widgets.text("num_members", "10000", "Number of Members")
dbutils.widgets.text("num_digital_journeys", "50000", "Number of Digital Journeys")
dbutils.widgets.text("num_calls", "12000", "Number of Call Logs")

# Get widget values
OUTPUT_PATH = dbutils.widgets.get("output_path")
NUM_MEMBERS = int(dbutils.widgets.get("num_members"))
NUM_DIGITAL_JOURNEYS = int(dbutils.widgets.get("num_digital_journeys"))
NUM_CALLS = int(dbutils.widgets.get("num_calls"))

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

print(f"Configuration:")
print(f"  Output Path: {OUTPUT_PATH}")
print(f"  Number of Members: {NUM_MEMBERS:,}")
print(f"  Number of Digital Journeys: {NUM_DIGITAL_JOURNEYS:,}")
print(f"  Number of Call Logs: {NUM_CALLS:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schemas

# COMMAND ----------

member_profiles_schema = StructType([
    StructField("member_id", StringType(), True),
    StructField("plan_type", StringType(), True),
    StructField("tenure_months", IntegerType(), True),
    StructField("segment", StringType(), True),
    StructField("language_preference", StringType(), True),
    StructField("enrollment_date", DateType(), True)
])

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Member Profiles

# COMMAND ----------

def generate_member_profiles(num_members):
    """
    Generate member profile data with realistic distribution

    Target distribution:
    - MAPD New: 35% (higher risk segment)
    - MAPD Tenured: 30%
    - Commercial: 35%
    - Spanish speakers: 20% (higher in certain segments)

    Returns:
        list[dict]: List of member profile dictionaries
    """
    members = []

    for i in range(num_members):
        member_id = f"M-{100000 + i}"

        # Determine plan type and segment
        rand = random.random()
        if rand < 0.35:
            plan_type = "MAPD"
            segment = "new"
            tenure_months = random.randint(1, 12)
        elif rand < 0.65:
            plan_type = "MAPD"
            segment = "tenured"
            tenure_months = random.randint(13, 120)
        else:
            plan_type = "Commercial"
            segment = random.choice(["new", "tenured"])
            tenure_months = random.randint(1, 84) if segment == "new" else random.randint(13, 84)

        # Language preference (Spanish higher in certain segments)
        if segment == "new" and random.random() < 0.25:
            language_preference = "Spanish"
        elif random.random() < 0.15:
            language_preference = "Spanish"
        elif random.random() < 0.05:
            language_preference = "Other"
        else:
            language_preference = "English"

        enrollment_date = datetime.now() - timedelta(days=tenure_months * 30)

        members.append({
            "member_id": member_id,
            "plan_type": plan_type,
            "tenure_months": tenure_months,
            "segment": segment,
            "language_preference": language_preference,
            "enrollment_date": enrollment_date.date()
        })

    return members

members_data = generate_member_profiles(NUM_MEMBERS)
member_profiles_sdf = spark.createDataFrame(members_data, schema=member_profiles_schema)

print(f"Generated {len(members_data):,} member profiles")
print("\nSample data:")
member_profiles_sdf.show(5, truncate=False)
print("\nDistribution:")
member_profiles_sdf.groupBy("plan_type", "segment").count().orderBy("plan_type", "segment").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Digital Journeys

# COMMAND ----------

def generate_digital_journeys(members_data, num_journeys):
    """
    Generate digital journey data with realistic leakage patterns

    Key patterns:
    - Billing abandons: 5,000 (primarily MAPD New on mobile)
    - ID card failures: 3,000 (Spanish speakers)
    - Pharmacy issues: 4,000 (mixed segments)
    - Remaining: normal successful journeys

    Args:
        members_data: List of member profile dicts
        num_journeys: Target number of journey events

    Returns:
        list[dict]: List of digital journey dictionaries
    """
    journeys = []
    member_ids = [m["member_id"] for m in members_data]

    # Error messages for different journey types
    error_messages = {
        "billing": [
            "Your plan is not eligible for this option",
            "Payment method not accepted",
            "Session timeout",
            "Server error occurred"
        ],
        "id_card": [
            "Download failed - try again",
            "File format not supported",
            "Access denied",
            "Network timeout"
        ],
        "pharmacy": [
            "Prescription not found",
            "Refill not eligible",
            "Pharmacy not in network",
            "Prior authorization required"
        ]
    }

    # Generate leakage journeys first (to ensure we hit targets)
    base_date = datetime.now() - timedelta(days=1)  # Yesterday's data

    # 1. Billing abandons (5,000) - MAPD New, primarily mobile
    mapd_new_members = [m["member_id"] for m in members_data
                        if m["plan_type"] == "MAPD" and m["segment"] == "new"]

    for i in range(5000):
        member_id = random.choice(mapd_new_members)
        session_id = f"SES-B-{i}"
        journey_start = base_date + timedelta(hours=random.randint(6, 22), minutes=random.randint(0, 59))
        device = random.choice(["mobile"] * 8 + ["desktop"] * 2)  # 80% mobile

        # Login
        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start,
            "journey_type": "billing",
            "action": "login",
            "device_type": device,
            "error_code": None,
            "error_message": None
        })

        # Page view
        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=2),
            "journey_type": "billing",
            "action": "page_view",
            "device_type": device,
            "error_code": None,
            "error_message": None
        })

        # Error and abandon
        error_msg = random.choice(error_messages["billing"])
        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=3),
            "journey_type": "billing",
            "action": "error",
            "device_type": device,
            "error_code": "ERR_BILLING_ELIGIBILITY" if "eligible" in error_msg else "ERR_BILLING_GENERAL",
            "error_message": error_msg
        })

        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=4),
            "journey_type": "billing",
            "action": "abandon",
            "device_type": device,
            "error_code": None,
            "error_message": None
        })

    # 2. ID card failures (3,000) - Spanish speakers
    spanish_members = [m["member_id"] for m in members_data
                       if m["language_preference"] == "Spanish"]

    for i in range(3000):
        member_id = random.choice(spanish_members)
        session_id = f"SES-I-{i}"
        journey_start = base_date + timedelta(hours=random.randint(6, 22), minutes=random.randint(0, 59))
        device = random.choice(["mobile", "desktop", "tablet"])

        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start,
            "journey_type": "id_card",
            "action": "login",
            "device_type": device,
            "error_code": None,
            "error_message": None
        })

        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=1),
            "journey_type": "id_card",
            "action": "page_view",
            "device_type": device,
            "error_code": None,
            "error_message": None
        })

        error_msg = random.choice(error_messages["id_card"])
        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=2),
            "journey_type": "id_card",
            "action": "error",
            "device_type": device,
            "error_code": "ERR_IDCARD_DOWNLOAD",
            "error_message": error_msg
        })

        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=3),
            "journey_type": "id_card",
            "action": "abandon",
            "device_type": device,
            "error_code": None,
            "error_message": None
        })

    # 3. Pharmacy escalations (4,000) - Mixed segments
    for i in range(4000):
        member_id = random.choice(member_ids)
        session_id = f"SES-P-{i}"
        journey_start = base_date + timedelta(hours=random.randint(6, 22), minutes=random.randint(0, 59))
        device = random.choice(["mobile", "desktop", "tablet"])

        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start,
            "journey_type": "pharmacy",
            "action": "login",
            "device_type": device,
            "error_code": None,
            "error_message": None
        })

        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=2),
            "journey_type": "pharmacy",
            "action": "page_view",
            "device_type": device,
            "error_code": None,
            "error_message": None
        })

        error_msg = random.choice(error_messages["pharmacy"])
        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=4),
            "journey_type": "pharmacy",
            "action": "error",
            "device_type": device,
            "error_code": "ERR_PHARMACY_REFILL",
            "error_message": error_msg
        })

        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=5),
            "journey_type": "pharmacy",
            "action": "abandon",
            "device_type": device,
            "error_code": None,
            "error_message": None
        })

    # 4. Fill remaining with normal successful journeys
    remaining_journeys = num_journeys - len(journeys)
    journey_types = ["billing", "id_card", "pharmacy"]
    device_types = ["mobile", "desktop", "tablet"]

    for i in range(remaining_journeys):
        member_id = random.choice(member_ids)
        session_id = f"SES-N-{i}"
        journey_type = random.choice(journey_types)
        device_type = random.choice(device_types)
        journey_start = base_date + timedelta(hours=random.randint(6, 22), minutes=random.randint(0, 59))

        # Successful journey
        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start,
            "journey_type": journey_type,
            "action": "login",
            "device_type": device_type,
            "error_code": None,
            "error_message": None
        })

        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=random.randint(1, 3)),
            "journey_type": journey_type,
            "action": "page_view",
            "device_type": device_type,
            "error_code": None,
            "error_message": None
        })

    return journeys

journeys_data = generate_digital_journeys(members_data, NUM_DIGITAL_JOURNEYS)
digital_journeys_sdf = spark.createDataFrame(journeys_data, schema=digital_journeys_schema)

print(f"Generated {len(journeys_data):,} digital journey events")
print("\nSample data:")
digital_journeys_sdf.show(10, truncate=False)
print("\nJourney type distribution:")
digital_journeys_sdf.groupBy("journey_type").count().orderBy("journey_type").show()
print("\nAction distribution:")
digital_journeys_sdf.groupBy("action").count().orderBy("action").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Call Logs

# COMMAND ----------

def generate_call_logs(members_data, journeys_data, num_calls):
    """
    Generate call logs with realistic leakage correlation

    Key patterns:
    - 12,000 calls total
    - ~5,000 from billing abandons (within 30 min)
    - ~3,000 from ID card failures (within 30 min)
    - ~4,000 from pharmacy issues (within 30 min)
    - MAPD New: higher repeat rate (28%), worse sentiment (-0.65)

    Args:
        members_data: List of member profile dicts
        journeys_data: List of digital journey dicts
        num_calls: Target number of call logs

    Returns:
        list[dict]: List of call log dictionaries
    """
    calls = []

    # Build member lookup dict
    member_dict = {m["member_id"]: m for m in members_data}

    # Get abandon/error journeys for leakage generation
    leakage_journeys = [j for j in journeys_data if j["action"] in ("abandon", "error")]

    # Helper: deduplicate by session_id and filter by journey_type, take up to N
    def get_unique_sessions(journeys, journey_type, limit):
        seen_sessions = set()
        result = []
        for j in journeys:
            if j["journey_type"] == journey_type and j["session_id"] not in seen_sessions:
                seen_sessions.add(j["session_id"])
                result.append(j)
                if len(result) >= limit:
                    break
        return result

    # 1. Generate calls from billing abandons (5,000)
    billing_abandons = get_unique_sessions(leakage_journeys, "billing", 5000)

    for i, row in enumerate(billing_abandons):
        call_id = f"CALL-B-{i}"
        member_id = row["member_id"]
        member_info = member_dict.get(member_id, {})

        # Call occurs 5-25 minutes after abandon
        call_time = row["timestamp"] + timedelta(minutes=random.randint(5, 25))

        # MAPD New members have worse sentiment and longer handle time
        is_mapd_new = (member_info.get("plan_type") == "MAPD" and
                       member_info.get("segment") == "new")

        if is_mapd_new:
            sentiment = random.uniform(-0.9, -0.4)  # Avg ~-0.65
            handle_time = random.randint(900, 1800)  # 15-30 min
        else:
            sentiment = random.uniform(-0.5, -0.1)  # Avg ~-0.3
            handle_time = random.randint(600, 1200)  # 10-20 min

        calls.append({
            "call_id": call_id,
            "member_id": member_id,
            "call_timestamp": call_time,
            "ivr_path": "main > billing > agent",
            "handle_time_seconds": handle_time,
            "agent_id": f"AGT-{random.randint(1000, 1999)}",
            "call_reason": "billing_question",
            "sentiment_score": round(sentiment, 2),
            "resolution_status": random.choice(["resolved", "escalated", "pending"])
        })

    # 2. Generate calls from ID card failures (3,000)
    id_card_errors = get_unique_sessions(leakage_journeys, "id_card", 3000)

    for i, row in enumerate(id_card_errors):
        call_id = f"CALL-I-{i}"
        member_id = row["member_id"]
        call_time = row["timestamp"] + timedelta(minutes=random.randint(5, 25))

        calls.append({
            "call_id": call_id,
            "member_id": member_id,
            "call_timestamp": call_time,
            "ivr_path": "main > id_card > agent",
            "handle_time_seconds": random.randint(300, 900),  # 5-15 min
            "agent_id": f"AGT-{random.randint(1000, 1999)}",
            "call_reason": "id_card_request",
            "sentiment_score": round(random.uniform(-0.4, 0.1), 2),
            "resolution_status": random.choice(["resolved", "escalated"])
        })

    # 3. Generate calls from pharmacy issues (4,000)
    pharmacy_errors = get_unique_sessions(leakage_journeys, "pharmacy", 4000)

    for i, row in enumerate(pharmacy_errors):
        call_id = f"CALL-P-{i}"
        member_id = row["member_id"]
        call_time = row["timestamp"] + timedelta(minutes=random.randint(5, 25))

        calls.append({
            "call_id": call_id,
            "member_id": member_id,
            "call_timestamp": call_time,
            "ivr_path": "main > pharmacy > agent",
            "handle_time_seconds": random.randint(600, 1500),  # 10-25 min
            "agent_id": f"AGT-{random.randint(1000, 1999)}",
            "call_reason": "pharmacy_refill",
            "sentiment_score": round(random.uniform(-0.3, 0.2), 2),
            "resolution_status": random.choice(["resolved", "escalated", "pending"])
        })

    # Add repeat calls (28% for MAPD New, 15% for others)
    mapd_new_calls = [c for c in calls
                      if member_dict.get(c["member_id"], {}).get("plan_type") == "MAPD"
                      and member_dict.get(c["member_id"], {}).get("segment") == "new"]

    # Generate repeat calls
    repeat_count = int(len(mapd_new_calls) * 0.28)
    for i in range(repeat_count):
        original_call = random.choice(mapd_new_calls)
        call_id = f"CALL-R-{i}"
        repeat_time = original_call["call_timestamp"] + timedelta(hours=random.randint(2, 48))

        calls.append({
            "call_id": call_id,
            "member_id": original_call["member_id"],
            "call_timestamp": repeat_time,
            "ivr_path": original_call["ivr_path"],
            "handle_time_seconds": random.randint(600, 1200),
            "agent_id": f"AGT-{random.randint(1000, 1999)}",
            "call_reason": original_call["call_reason"],
            "sentiment_score": round(random.uniform(-0.8, -0.5), 2),  # Even worse on repeat
            "resolution_status": random.choice(["resolved", "escalated"])
        })

    return calls

calls_data = generate_call_logs(members_data, journeys_data, NUM_CALLS)
call_logs_sdf = spark.createDataFrame(calls_data, schema=call_logs_schema)

print(f"Generated {len(calls_data):,} call logs")
print("\nSample data:")
call_logs_sdf.show(5, truncate=False)
print("\nCall reason distribution:")
call_logs_sdf.groupBy("call_reason").count().orderBy("call_reason").show()
print("\nAverage sentiment by call reason:")
call_logs_sdf.groupBy("call_reason").agg(
    F.round(F.avg("sentiment_score"), 2).alias("avg_sentiment")
).orderBy("call_reason").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Data to Files

# COMMAND ----------

# Convert pandas DataFrames to Spark DataFrames and save as CSV
member_profiles_spark_df = spark.createDataFrame(member_profiles_df)
digital_journeys_spark_df = spark.createDataFrame(digital_journeys_df)
call_logs_spark_df = spark.createDataFrame(call_logs_df)


# COMMAND ----------

print(OUTPUT_PATH)

# COMMAND ----------

# For smaller datasets (< 1M rows)
member_profiles_spark_df.toPandas().to_csv(
    f"/Volumes/insurance_final_with_ai/default/data/inputpath/member_profiles.csv", 
    index=False
)

digital_journeys_spark_df.toPandas().to_csv(
    f"/Volumes/insurance_final_with_ai/default/data/inputpath/digital_journeys.csv", 
    index=False
)

call_logs_spark_df.toPandas().to_csv(
    f"/Volumes/insurance_final_with_ai/default/data/inputpath/call_logs.csv", 
    index=False
)

save_as_csv(member_profiles_sdf, f"{OUTPUT_PATH}/member_profiles.csv")
save_as_csv(digital_journeys_sdf, f"{OUTPUT_PATH}/digital_journeys.csv")
save_as_csv(call_logs_sdf, f"{OUTPUT_PATH}/call_logs.csv")

# COMMAND ----------

print("Data saved successfully!")
print(f"\nFiles created:")
print(f"  - {OUTPUT_PATH}member_profiles.csv ({len(member_profiles_df):,} records)")
print(f"  - {OUTPUT_PATH}digital_journeys.csv ({len(digital_journeys_df):,} records)")
print(f"  - {OUTPUT_PATH}call_logs.csv ({len(call_logs_df):,} records)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

print("=" * 80)
print("DATA GENERATION SUMMARY")
print("=" * 80)

# 1. Member Profiles
total_members = member_profiles_sdf.count()
print(f"\n1. MEMBER PROFILES")
print(f"   Total Members: {total_members:,}")
print(f"   Distribution by segment:")
segment_counts = (member_profiles_sdf
    .groupBy("plan_type", "segment")
    .count()
    .withColumn("pct", F.round(F.col("count") / F.lit(total_members) * 100, 1))
    .orderBy("plan_type", "segment")
    .collect()
)
for row in segment_counts:
    print(f"     - {row.plan_type} {row.segment}: {row['count']:,} ({row.pct:.1f}%)")

# 2. Digital Journeys
total_events = digital_journeys_sdf.count()
unique_sessions = digital_journeys_sdf.select("session_id").distinct().count()
abandon_count = digital_journeys_sdf.filter(F.col("action") == "abandon").count()
error_count = digital_journeys_sdf.filter(F.col("action") == "error").count()

print(f"\n2. DIGITAL JOURNEYS")
print(f"   Total Events: {total_events:,}")
print(f"   Unique Sessions: {unique_sessions:,}")
print(f"   Abandons: {abandon_count:,}")
print(f"   Errors: {error_count:,}")
print(f"   Journey type breakdown:")
journey_counts = (digital_journeys_sdf
    .groupBy("journey_type")
    .count()
    .orderBy("journey_type")
    .collect()
)
for row in journey_counts:
    print(f"     - {row.journey_type}: {row['count']:,}")

# 3. Call Logs
call_stats = call_logs_sdf.agg(
    F.count("*").alias("total_calls"),
    F.avg("handle_time_seconds").alias("avg_handle_time"),
    F.avg("sentiment_score").alias("avg_sentiment")
).collect()[0]

print(f"\n3. CALL LOGS")
print(f"   Total Calls: {call_stats.total_calls:,}")
print(f"   Average Handle Time: {call_stats.avg_handle_time/60:.1f} minutes")
print(f"   Average Sentiment: {call_stats.avg_sentiment:.2f}")
print(f"   Call reason breakdown:")
reason_stats = (call_logs_sdf
    .groupBy("call_reason")
    .agg(
        F.count("*").alias("count"),
        F.round(F.avg("sentiment_score"), 2).alias("avg_sentiment")
    )
    .orderBy("call_reason")
    .collect()
)
for row in reason_stats:
    print(f"     - {row.call_reason}: {row['count']:,} calls (avg sentiment: {row.avg_sentiment:.2f})")

# 4. Expected Leakage Patterns
print(f"\n4. EXPECTED LEAKAGE PATTERNS")
print(f"   Billing abandons → calls: ~5,000 (MAPD New segment)")
print(f"   ID card failures → calls: ~3,000 (Spanish speakers)")
print(f"   Pharmacy issues → calls: ~4,000 (Mixed segments)")
print(f"   Total expected leakage calls: ~12,000")
print(f"   Cost per call: $8")
print(f"   Total daily cost: ~${call_stats.total_calls * 8:,}")

print("\n" + "=" * 80)
print("Data generation complete! Ready for Bronze ingestion.")
print("=" * 80)
