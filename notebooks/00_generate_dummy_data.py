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
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for configuration
dbutils.widgets.text("output_path", "/dbfs/FileStore/demo_data/sample", "Output Path")
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
    
    return pd.DataFrame(members)

member_profiles_df = generate_member_profiles(NUM_MEMBERS)
print(f"Generated {len(member_profiles_df):,} member profiles")
print("\nSample data:")
print(member_profiles_df.head())
print("\nDistribution:")
print(member_profiles_df.groupby(['plan_type', 'segment']).size())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Digital Journeys

# COMMAND ----------

def generate_digital_journeys(member_profiles_df, num_journeys):
    """
    Generate digital journey data with realistic leakage patterns
    
    Key patterns:
    - Billing abandons: 5,000 (primarily MAPD New on mobile)
    - ID card failures: 3,000 (Spanish speakers)
    - Pharmacy issues: 4,000 (mixed segments)
    - Remaining: normal successful journeys
    """
    journeys = []
    member_ids = member_profiles_df['member_id'].tolist()
    
    # Define journey patterns
    journey_types = ["billing", "id_card", "pharmacy"]
    actions = ["login", "page_view", "abandon", "error"]
    device_types = ["mobile", "desktop", "tablet"]
    
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
    mapd_new_members = member_profiles_df[
        (member_profiles_df['plan_type'] == 'MAPD') & 
        (member_profiles_df['segment'] == 'new')
    ]['member_id'].tolist()
    
    for i in range(5000):
        member_id = random.choice(mapd_new_members)
        session_id = f"SES-B-{i}"
        journey_start = base_date + timedelta(hours=random.randint(6, 22), minutes=random.randint(0, 59))
        
        # Login
        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start,
            "journey_type": "billing",
            "action": "login",
            "device_type": random.choice(["mobile"] * 8 + ["desktop"] * 2),  # 80% mobile
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
            "device_type": journeys[-1]["device_type"],
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
            "device_type": journeys[-1]["device_type"],
            "error_code": "ERR_BILLING_ELIGIBILITY" if "eligible" in error_msg else "ERR_BILLING_GENERAL",
            "error_message": error_msg
        })
        
        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=4),
            "journey_type": "billing",
            "action": "abandon",
            "device_type": journeys[-1]["device_type"],
            "error_code": None,
            "error_message": None
        })
    
    # 2. ID card failures (3,000) - Spanish speakers
    spanish_members = member_profiles_df[
        member_profiles_df['language_preference'] == 'Spanish'
    ]['member_id'].tolist()
    
    for i in range(3000):
        member_id = random.choice(spanish_members)
        session_id = f"SES-I-{i}"
        journey_start = base_date + timedelta(hours=random.randint(6, 22), minutes=random.randint(0, 59))
        
        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start,
            "journey_type": "id_card",
            "action": "login",
            "device_type": random.choice(device_types),
            "error_code": None,
            "error_message": None
        })
        
        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=1),
            "journey_type": "id_card",
            "action": "page_view",
            "device_type": journeys[-1]["device_type"],
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
            "device_type": journeys[-1]["device_type"],
            "error_code": "ERR_IDCARD_DOWNLOAD",
            "error_message": error_msg
        })
        
        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=3),
            "journey_type": "id_card",
            "action": "abandon",
            "device_type": journeys[-1]["device_type"],
            "error_code": None,
            "error_message": None
        })
    
    # 3. Pharmacy escalations (4,000) - Mixed segments
    for i in range(4000):
        member_id = random.choice(member_ids)
        session_id = f"SES-P-{i}"
        journey_start = base_date + timedelta(hours=random.randint(6, 22), minutes=random.randint(0, 59))
        
        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start,
            "journey_type": "pharmacy",
            "action": "login",
            "device_type": random.choice(device_types),
            "error_code": None,
            "error_message": None
        })
        
        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=2),
            "journey_type": "pharmacy",
            "action": "page_view",
            "device_type": journeys[-1]["device_type"],
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
            "device_type": journeys[-1]["device_type"],
            "error_code": "ERR_PHARMACY_REFILL",
            "error_message": error_msg
        })
        
        journeys.append({
            "member_id": member_id,
            "session_id": session_id,
            "timestamp": journey_start + timedelta(minutes=5),
            "journey_type": "pharmacy",
            "action": "abandon",
            "device_type": journeys[-1]["device_type"],
            "error_code": None,
            "error_message": None
        })
    
    # 4. Fill remaining with normal successful journeys
    remaining_journeys = num_journeys - len(journeys)
    
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
    
    return pd.DataFrame(journeys)

digital_journeys_df = generate_digital_journeys(member_profiles_df, NUM_DIGITAL_JOURNEYS)
print(f"Generated {len(digital_journeys_df):,} digital journey events")
print("\nSample data:")
print(digital_journeys_df.head(10))
print("\nJourney type distribution:")
print(digital_journeys_df.groupby('journey_type').size())
print("\nAction distribution:")
print(digital_journeys_df.groupby('action').size())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Call Logs

# COMMAND ----------

def generate_call_logs(member_profiles_df, digital_journeys_df, num_calls):
    """
    Generate call logs with realistic leakage correlation
    
    Key patterns:
    - 12,000 calls total
    - ~5,000 from billing abandons (within 30 min)
    - ~3,000 from ID card failures (within 30 min)
    - ~4,000 from pharmacy issues (within 30 min)
    - MAPD New: higher repeat rate (28%), worse sentiment (-0.65)
    """
    calls = []
    
    # Get member profiles with segment info
    member_dict = member_profiles_df.set_index('member_id').to_dict('index')
    
    # Get abandon/error journeys for leakage generation
    leakage_journeys = digital_journeys_df[
        digital_journeys_df['action'].isin(['abandon', 'error'])
    ].copy()
    
    # 1. Generate calls from billing abandons (5,000)
    billing_abandons = leakage_journeys[
        leakage_journeys['journey_type'] == 'billing'
    ].drop_duplicates('session_id').head(5000)
    
    for idx, row in billing_abandons.iterrows():
        call_id = f"CALL-B-{idx}"
        member_id = row['member_id']
        member_info = member_dict.get(member_id, {})
        
        # Call occurs 5-25 minutes after abandon
        call_time = row['timestamp'] + timedelta(minutes=random.randint(5, 25))
        
        # MAPD New members have worse sentiment and longer handle time
        is_mapd_new = (member_info.get('plan_type') == 'MAPD' and 
                      member_info.get('segment') == 'new')
        
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
    id_card_errors = leakage_journeys[
        leakage_journeys['journey_type'] == 'id_card'
    ].drop_duplicates('session_id').head(3000)
    
    for idx, row in id_card_errors.iterrows():
        call_id = f"CALL-I-{idx}"
        member_id = row['member_id']
        call_time = row['timestamp'] + timedelta(minutes=random.randint(5, 25))
        
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
    pharmacy_errors = leakage_journeys[
        leakage_journeys['journey_type'] == 'pharmacy'
    ].drop_duplicates('session_id').head(4000)
    
    for idx, row in pharmacy_errors.iterrows():
        call_id = f"CALL-P-{idx}"
        member_id = row['member_id']
        call_time = row['timestamp'] + timedelta(minutes=random.randint(5, 25))
        
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
    mapd_new_calls = [c for c in calls if member_dict.get(c['member_id'], {}).get('plan_type') == 'MAPD' 
                      and member_dict.get(c['member_id'], {}).get('segment') == 'new']
    
    # Generate repeat calls
    repeat_count = int(len(mapd_new_calls) * 0.28)
    for i in range(repeat_count):
        original_call = random.choice(mapd_new_calls)
        call_id = f"CALL-R-{i}"
        repeat_time = original_call['call_timestamp'] + timedelta(hours=random.randint(2, 48))
        
        calls.append({
            "call_id": call_id,
            "member_id": original_call['member_id'],
            "call_timestamp": repeat_time,
            "ivr_path": original_call['ivr_path'],
            "handle_time_seconds": random.randint(600, 1200),
            "agent_id": f"AGT-{random.randint(1000, 1999)}",
            "call_reason": original_call['call_reason'],
            "sentiment_score": round(random.uniform(-0.8, -0.5), 2),  # Even worse on repeat
            "resolution_status": random.choice(["resolved", "escalated"])
        })
    
    return pd.DataFrame(calls)

call_logs_df = generate_call_logs(member_profiles_df, digital_journeys_df, NUM_CALLS)
print(f"Generated {len(call_logs_df):,} call logs")
print("\nSample data:")
print(call_logs_df.head())
print("\nCall reason distribution:")
print(call_logs_df.groupby('call_reason').size())
print("\nAverage sentiment by call reason:")
print(call_logs_df.groupby('call_reason')['sentiment_score'].mean())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Data to Files

# COMMAND ----------

# Save to CSV files
member_profiles_df.to_csv(f"{OUTPUT_PATH}/member_profiles.csv", index=False)
digital_journeys_df.to_csv(f"{OUTPUT_PATH}/digital_journeys.csv", index=False)
call_logs_df.to_csv(f"{OUTPUT_PATH}/call_logs.csv", index=False)

print("Data saved successfully!")
print(f"\nFiles created:")
print(f"  - {OUTPUT_PATH}/member_profiles.csv ({len(member_profiles_df):,} records)")
print(f"  - {OUTPUT_PATH}/digital_journeys.csv ({len(digital_journeys_df):,} records)")
print(f"  - {OUTPUT_PATH}/call_logs.csv ({len(call_logs_df):,} records)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

print("=" * 80)
print("DATA GENERATION SUMMARY")
print("=" * 80)

print("\n1. MEMBER PROFILES")
print(f"   Total Members: {len(member_profiles_df):,}")
print(f"   Distribution by segment:")
for (plan, segment), count in member_profiles_df.groupby(['plan_type', 'segment']).size().items():
    pct = (count / len(member_profiles_df)) * 100
    print(f"     - {plan} {segment}: {count:,} ({pct:.1f}%)")

print("\n2. DIGITAL JOURNEYS")
print(f"   Total Events: {len(digital_journeys_df):,}")
print(f"   Unique Sessions: {digital_journeys_df['session_id'].nunique():,}")
abandon_count = len(digital_journeys_df[digital_journeys_df['action'] == 'abandon'])
error_count = len(digital_journeys_df[digital_journeys_df['action'] == 'error'])
print(f"   Abandons: {abandon_count:,}")
print(f"   Errors: {error_count:,}")
print(f"   Journey type breakdown:")
for journey_type, count in digital_journeys_df.groupby('journey_type').size().items():
    print(f"     - {journey_type}: {count:,}")

print("\n3. CALL LOGS")
print(f"   Total Calls: {len(call_logs_df):,}")
print(f"   Average Handle Time: {call_logs_df['handle_time_seconds'].mean()/60:.1f} minutes")
print(f"   Average Sentiment: {call_logs_df['sentiment_score'].mean():.2f}")
print(f"   Call reason breakdown:")
for reason, count in call_logs_df.groupby('call_reason').size().items():
    avg_sentiment = call_logs_df[call_logs_df['call_reason'] == reason]['sentiment_score'].mean()
    print(f"     - {reason}: {count:,} calls (avg sentiment: {avg_sentiment:.2f})")

print("\n4. EXPECTED LEAKAGE PATTERNS")
print(f"   Billing abandons → calls: ~5,000 (MAPD New segment)")
print(f"   ID card failures → calls: ~3,000 (Spanish speakers)")
print(f"   Pharmacy issues → calls: ~4,000 (Mixed segments)")
print(f"   Total expected leakage calls: ~12,000")
print(f"   Cost per call: $8")
print(f"   Total daily cost: ~${len(call_logs_df) * 8:,}")

print("\n" + "=" * 80)
print("Data generation complete! Ready for Bronze ingestion.")
print("=" * 80)
