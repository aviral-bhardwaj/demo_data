# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Run Pipeline
# MAGIC 
# MAGIC **Purpose**: Orchestrate the complete medallion architecture pipeline
# MAGIC 
# MAGIC **Pipeline Flow**:
# MAGIC 1. Generate dummy data (optional)
# MAGIC 2. Bronze layer ingestion
# MAGIC 3. Silver layer transformation
# MAGIC 4. Gold layer aggregations
# MAGIC 
# MAGIC **Features**:
# MAGIC - Sequential execution with error handling
# MAGIC - Logging and monitoring
# MAGIC - Support for full refresh vs incremental runs
# MAGIC - Performance metrics

# COMMAND ----------

# Import required libraries
from datetime import datetime
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for pipeline configuration
dbutils.widgets.dropdown("run_data_generator", "Yes", ["Yes", "No"], "Run Data Generator?")
dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema")
dbutils.widgets.text("silver_schema", "silver", "Silver Schema")
dbutils.widgets.text("gold_schema", "gold", "Gold Schema")
dbutils.widgets.text("input_path", "/dbfs/FileStore/demo_data/sample", "Input Data Path")
dbutils.widgets.text("num_members", "10000", "Number of Members")
dbutils.widgets.text("num_digital_journeys", "50000", "Number of Digital Journeys")
dbutils.widgets.text("num_calls", "12000", "Number of Call Logs")

# Get widget values
RUN_DATA_GENERATOR = dbutils.widgets.get("run_data_generator") == "Yes"
CATALOG = dbutils.widgets.get("catalog_name")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")
INPUT_PATH = dbutils.widgets.get("input_path")
NUM_MEMBERS = dbutils.widgets.get("num_members")
NUM_DIGITAL_JOURNEYS = dbutils.widgets.get("num_digital_journeys")
NUM_CALLS = dbutils.widgets.get("num_calls")

print("=" * 80)
print("DIGITAL-TO-CALL LEAKAGE DETECTION PIPELINE")
print("=" * 80)
print(f"\nPipeline Configuration:")
print(f"  Run Data Generator: {RUN_DATA_GENERATOR}")
print(f"  Catalog: {CATALOG}")
print(f"  Bronze Schema: {BRONZE_SCHEMA}")
print(f"  Silver Schema: {SILVER_SCHEMA}")
print(f"  Gold Schema: {GOLD_SCHEMA}")
print(f"  Input Path: {INPUT_PATH}")
if RUN_DATA_GENERATOR:
    print(f"  Members: {NUM_MEMBERS}")
    print(f"  Digital Journeys: {NUM_DIGITAL_JOURNEYS}")
    print(f"  Call Logs: {NUM_CALLS}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Execution Functions

# COMMAND ----------

class PipelineLogger:
    """Simple logger for pipeline execution"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.steps = []
    
    def log_step_start(self, step_name):
        """Log the start of a pipeline step"""
        print(f"\n{'='*80}")
        print(f"STEP: {step_name}")
        print(f"{'='*80}")
        print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return time.time()
    
    def log_step_end(self, step_name, start_time, success=True):
        """Log the end of a pipeline step"""
        duration = time.time() - start_time
        status = "✓ SUCCESS" if success else "✗ FAILED"
        
        self.steps.append({
            "step": step_name,
            "status": status,
            "duration_seconds": duration
        })
        
        print(f"\nStatus: {status}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"{'='*80}")
    
    def print_summary(self):
        """Print pipeline execution summary"""
        total_duration = (datetime.now() - self.start_time).total_seconds()
        
        print(f"\n{'='*80}")
        print("PIPELINE EXECUTION SUMMARY")
        print(f"{'='*80}")
        print(f"\nTotal Duration: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
        print(f"\nStep Results:")
        
        for step in self.steps:
            print(f"  {step['status']:<15} {step['step']:<50} ({step['duration_seconds']:.2f}s)")
        
        all_success = all(s['status'] == "✓ SUCCESS" for s in self.steps)
        
        print(f"\n{'='*80}")
        if all_success:
            print("✓ PIPELINE COMPLETED SUCCESSFULLY")
        else:
            print("✗ PIPELINE COMPLETED WITH ERRORS")
        print(f"{'='*80}")

# Initialize logger
logger = PipelineLogger()

# COMMAND ----------

def run_notebook(notebook_path, timeout_seconds, parameters=None):
    """
    Execute a notebook with error handling
    
    Args:
        notebook_path: Path to the notebook
        timeout_seconds: Timeout in seconds
        parameters: Dictionary of parameters to pass
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if parameters is None:
            parameters = {}
        
        result = dbutils.notebook.run(notebook_path, timeout_seconds, parameters)
        return True
    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Generate Dummy Data (Optional)

# COMMAND ----------

if RUN_DATA_GENERATOR:
    step_start = logger.log_step_start("Generate Dummy Data")
    
    success = run_notebook(
        "./00_generate_dummy_data",
        timeout_seconds=600,
        parameters={
            "output_path": INPUT_PATH,
            "num_members": NUM_MEMBERS,
            "num_digital_journeys": NUM_DIGITAL_JOURNEYS,
            "num_calls": NUM_CALLS
        }
    )
    
    logger.log_step_end("Generate Dummy Data", step_start, success)
    
    if not success:
        dbutils.notebook.exit("Pipeline failed at data generation step")
else:
    print("\nSkipping data generation (using existing data)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Bronze Layer Ingestion

# COMMAND ----------

step_start = logger.log_step_start("Bronze Layer Ingestion")

success = run_notebook(
    "./01_bronze_ingestion",
    timeout_seconds=600,
    parameters={
        "catalog_name": CATALOG,
        "bronze_schema": BRONZE_SCHEMA,
        "input_path": INPUT_PATH
    }
)

logger.log_step_end("Bronze Layer Ingestion", step_start, success)

if not success:
    dbutils.notebook.exit("Pipeline failed at bronze ingestion step")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Silver Layer Transformation

# COMMAND ----------

step_start = logger.log_step_start("Silver Layer Transformation")

success = run_notebook(
    "./02_silver_transformation",
    timeout_seconds=900,
    parameters={
        "catalog_name": CATALOG,
        "bronze_schema": BRONZE_SCHEMA,
        "silver_schema": SILVER_SCHEMA,
        "leakage_window_minutes": "30"
    }
)

logger.log_step_end("Silver Layer Transformation", step_start, success)

if not success:
    dbutils.notebook.exit("Pipeline failed at silver transformation step")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Gold Layer Aggregations

# COMMAND ----------

step_start = logger.log_step_start("Gold Layer Aggregations")

success = run_notebook(
    "./03_gold_aggregations",
    timeout_seconds=900,
    parameters={
        "catalog_name": CATALOG,
        "silver_schema": SILVER_SCHEMA,
        "gold_schema": GOLD_SCHEMA,
        "cost_per_call": "8.0"
    }
)

logger.log_step_end("Gold Layer Aggregations", step_start, success)

if not success:
    dbutils.notebook.exit("Pipeline failed at gold aggregations step")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

# Print execution summary
logger.print_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation

# COMMAND ----------

print("\n" + "=" * 80)
print("DATA QUALITY VALIDATION")
print("=" * 80)

# Validate record counts across layers
print("\n1. Record Counts by Layer:")
print("-" * 80)

try:
    # Bronze layer
    bronze_digital = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.digital_journeys_raw").count()
    bronze_calls = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.call_logs_raw").count()
    bronze_members = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.member_profiles_raw").count()
    
    print(f"Bronze Layer:")
    print(f"  - digital_journeys_raw: {bronze_digital:,} records")
    print(f"  - call_logs_raw: {bronze_calls:,} records")
    print(f"  - member_profiles_raw: {bronze_members:,} records")
    
    # Silver layer
    silver_digital = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.digital_journeys_cleaned").count()
    silver_calls = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.call_logs_cleaned").count()
    silver_members = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.member_profiles_enriched").count()
    silver_leakage = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.digital_call_leakage").count()
    
    print(f"\nSilver Layer:")
    print(f"  - digital_journeys_cleaned: {silver_digital:,} records")
    print(f"  - call_logs_cleaned: {silver_calls:,} records")
    print(f"  - member_profiles_enriched: {silver_members:,} records")
    print(f"  - digital_call_leakage: {silver_leakage:,} records")
    
    # Gold layer
    gold_daily = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.daily_leakage_summary").count()
    gold_segment = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.segment_drilldown").count()
    gold_timeline = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.journey_timeline").count()
    gold_root_cause = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.root_cause_analysis").count()
    gold_savings = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.savings_model").count()
    gold_actions = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.action_recommendations").count()
    
    print(f"\nGold Layer:")
    print(f"  - daily_leakage_summary: {gold_daily:,} records")
    print(f"  - segment_drilldown: {gold_segment:,} records")
    print(f"  - journey_timeline: {gold_timeline:,} records")
    print(f"  - root_cause_analysis: {gold_root_cause:,} records")
    print(f"  - savings_model: {gold_savings:,} records")
    print(f"  - action_recommendations: {gold_actions:,} records")
    
    print("\n✓ All tables validated successfully")
    
except Exception as e:
    print(f"\n✗ Validation error: {str(e)}")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Business Metrics

# COMMAND ----------

print("\n" + "=" * 80)
print("KEY BUSINESS METRICS")
print("=" * 80)

try:
    # Total leakage and cost
    from pyspark.sql import functions as F
    
    leakage_metrics = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.daily_leakage_summary").agg(
        F.sum("daily_calls").alias("total_calls"),
        F.sum("daily_cost").alias("total_cost")
    ).collect()[0]
    
    total_calls = leakage_metrics.total_calls
    total_cost = leakage_metrics.total_cost
    
    print(f"\n1. Overall Leakage:")
    print(f"   - Total daily calls: {total_calls:,.0f}")
    print(f"   - Total daily cost: ${total_cost:,.0f}")
    print(f"   - Annualized cost: ${total_cost * 365:,.0f}")
    
    # Breakdown by journey type
    print(f"\n2. Leakage by Journey Type:")
    journey_breakdown = spark.sql(f"""
        SELECT 
            leakage_type,
            SUM(daily_calls) as calls,
            SUM(daily_cost) as cost,
            MAX(trend_pct) as trend
        FROM {CATALOG}.{GOLD_SCHEMA}.daily_leakage_summary
        GROUP BY leakage_type
        ORDER BY calls DESC
    """)
    
    for row in journey_breakdown.collect():
        print(f"   - {row.leakage_type}: {row.calls:,.0f} calls, ${row.cost:,.0f} cost, {row.trend:+.1f}% trend")
    
    # Savings opportunity
    print(f"\n3. Savings Opportunity:")
    savings_data = spark.sql(f"""
        SELECT 
            SUM(fix_rate_10_pct_savings) as savings_10,
            SUM(fix_rate_25_pct_savings) as savings_25,
            SUM(fix_rate_50_pct_savings) as savings_50
        FROM {CATALOG}.{GOLD_SCHEMA}.savings_model
    """).collect()[0]
    
    print(f"   - 10% improvement: ${savings_data.savings_10:,.0f} annually")
    print(f"   - 25% improvement: ${savings_data.savings_25:,.0f} annually")
    print(f"   - 50% improvement: ${savings_data.savings_50:,.0f} annually")
    
    # Top actions
    print(f"\n4. Top Priority Actions:")
    top_actions = spark.sql(f"""
        SELECT 
            action_title,
            estimated_savings_annual,
            effort_estimate
        FROM {CATALOG}.{GOLD_SCHEMA}.action_recommendations
        ORDER BY priority_rank
        LIMIT 3
    """)
    
    for row in top_actions.collect():
        print(f"   - {row.action_title}")
        print(f"     Savings: ${row.estimated_savings_annual/1000000:.1f}M, Effort: {row.effort_estimate}")
    
    print("\n✓ Metrics calculated successfully")
    
except Exception as e:
    print(f"\n✗ Metrics calculation error: {str(e)}")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps

# COMMAND ----------

print("\n" + "=" * 80)
print("NEXT STEPS")
print("=" * 80)

print("""
✓ Pipeline execution complete!

Next steps to operationalize:

1. DASHBOARD CREATION
   - Use Databricks SQL or Lakeview to create dashboards
   - Connect to Gold layer tables
   - Visualize Executive Overview, Segment Drill-Down, and other views
   
2. SCHEDULING
   - Set up Databricks Jobs to run this pipeline daily
   - Configure alerts for pipeline failures
   - Monitor execution times and optimize as needed
   
3. GENIE INTEGRATION
   - Enable Genie on the Gold schema for natural language queries
   - Test queries like "Show top digital billing failures today"
   - Configure for executive self-service
   
4. AGENT FRAMEWORK
   - Build Insight Scout agent to detect spikes automatically
   - Create Next-Best-Action agent for automated recommendations
   - Integrate with alerting system
   
5. INCREMENTAL PROCESSING
   - Modify notebooks to support incremental updates
   - Use Delta Lake change data feed
   - Optimize for streaming ingestion
   
6. DATA QUALITY MONITORING
   - Set up expectations using Great Expectations or Databricks
   - Monitor data quality metrics over time
   - Alert on anomalies

For questions or support, refer to the README.md file.
""")

print("=" * 80)

# COMMAND ----------

# Return success
dbutils.notebook.exit("Pipeline completed successfully")
