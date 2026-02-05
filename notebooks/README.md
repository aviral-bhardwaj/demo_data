# Digital-to-Call Leakage Detection - Databricks Notebooks

This directory contains Databricks Python notebooks implementing a complete medallion architecture (Bronze → Silver → Gold) for the Digital-to-Call Leakage Detection use case.

## Overview

These notebooks demonstrate how Databricks turns raw data into live insights → agentic actions → cost savings for a healthcare/insurance call center optimization scenario.

## Directory Structure

```
notebooks/
├── 00_generate_dummy_data.py      # Generate synthetic data
├── 01_bronze_ingestion.py         # Bronze layer (raw data)
├── 02_silver_transformation.py    # Silver layer (cleaned, enriched)
├── 03_gold_aggregations.py        # Gold layer (dashboard aggregations)
├── 04_run_pipeline.py             # Pipeline orchestration
└── config/
    └── schema_definitions.py      # Schema definitions for all layers
```

## Notebooks

### 00 - Generate Dummy Data

**Purpose**: Generate synthetic data using Python Faker library

**Data Generated**:
- **Digital Journeys** (~50,000 records): member_id, session_id, timestamp, journey_type, action, device_type, error_code, error_message
- **Call Logs** (~12,000 records): call_id, member_id, call_timestamp, ivr_path, handle_time_seconds, agent_id, call_reason, sentiment_score, resolution_status
- **Member Profiles** (~10,000 records): member_id, plan_type, tenure_months, segment, language_preference, enrollment_date

**Key Patterns**:
- 5,000 daily billing abandon → call (MAPD New segment, +14% trend)
- 3,000 daily ID card download fail → call (Spanish segment, +8% trend)
- 4,000 daily pharmacy refill → call escalate (Mixed segment, +3% trend)
- Cost per call: $8
- MAPD New members: 28% repeat call rate, -0.65 avg sentiment

**Parameters**:
- `output_path`: Directory to save generated CSV files (default: `/dbfs/FileStore/demo_data/sample`)
- `num_members`: Number of member profiles to generate (default: 10000)
- `num_digital_journeys`: Number of digital journey events (default: 50000)
- `num_calls`: Number of call log records (default: 12000)

### 01 - Bronze Layer Ingestion

**Purpose**: Ingest raw data files into Bronze layer Delta tables

**Bronze Layer Philosophy**:
- Minimal transformation
- Preserve raw data exactly as received
- Add ingestion metadata only (ingestion_timestamp, source_file)

**Tables Created**:
- `bronze.digital_journeys_raw`
- `bronze.call_logs_raw`
- `bronze.member_profiles_raw`

**Parameters**:
- `catalog_name`: Unity Catalog name (default: main)
- `bronze_schema`: Bronze schema name (default: bronze)
- `input_path`: Path to input CSV files (default: `/dbfs/FileStore/demo_data/sample`)

### 02 - Silver Layer Transformation

**Purpose**: Clean, enrich, and join data to identify digital-to-call leakage

**Silver Layer Philosophy**:
- Data cleansing (handle nulls, duplicates)
- Data type standardization
- Business logic application
- Enrichment with derived fields

**Tables Created**:
- `silver.digital_journeys_cleaned` - Cleaned digital events
- `silver.call_logs_cleaned` - Cleaned call records
- `silver.member_profiles_enriched` - Enriched member data with derived category
- `silver.digital_call_leakage` - Main analytical table with leakage detection

**Leakage Detection Logic**:
A digital journey is considered "leakage" if:
1. The journey ends in abandon or error action
2. The same member makes a call within 30 minutes (configurable)
3. The call reason aligns with the journey type

**Derived Fields**:
- `is_leakage_call`: Boolean flag
- `leakage_type`: Journey type (billing/id_card/pharmacy)
- `time_to_call_minutes`: Time between digital event and call
- `is_repeat_call`: Flag for repeat calls within 7 days
- `member_category`: Derived segment (MAPD New, MAPD Tenured, Commercial)

**Parameters**:
- `catalog_name`: Unity Catalog name (default: main)
- `bronze_schema`: Bronze schema name (default: bronze)
- `silver_schema`: Silver schema name (default: silver)
- `leakage_window_minutes`: Time window for leakage detection (default: 30)

### 03 - Gold Layer Aggregations

**Purpose**: Create aggregated tables for the Command Center Dashboard

**Gold Layer Philosophy**:
- Business-level aggregations
- Optimized for dashboard queries
- Pre-calculated metrics
- Denormalized for performance

**Tables Created**:

1. **`gold.daily_leakage_summary`** - Executive Overview
   - Journey type, daily calls, daily cost, segment, trend %
   - Supports "What's breaking right now?" view

2. **`gold.segment_drilldown`** - Segment Analysis
   - Breakdown by member category
   - Includes repeat call %, average sentiment
   - Supports "Who is this hurting?" view

3. **`gold.journey_timeline`** - Journey Playback
   - Sequential events for individual members
   - Combined digital and call events
   - Supports "Show me what this feels like" view

4. **`gold.root_cause_analysis`** - Root Cause
   - Error codes and device breakdown
   - Leakage counts and percentages
   - Trend by date
   - Supports "Why is this happening?" view

5. **`gold.savings_model`** - Financial Projections
   - Avoidable calls per day
   - Annualized costs
   - Savings at 10%, 25%, 50% fix rates
   - NPS lift estimates
   - Supports "Is this worth fixing?" view

6. **`gold.action_recommendations`** - Next-Best Actions
   - Prioritized action list
   - Estimated savings, effort, CX impact
   - Ownership assignment
   - Supports "What should my teams do?" view

**Key Metrics**:
- Total daily leakage: ~12K calls, $112K cost
- Billing abandon → call: 5K calls, $40K cost, MAPD New, +14%
- ID card repeats: 3K calls, $24K cost, Spanish, +8%
- Pharmacy escalation: 4K calls, $32K cost, Mixed, +3%
- Avoidable billing calls/day: 5,000 at $8/call = $14.6M annualized

**Parameters**:
- `catalog_name`: Unity Catalog name (default: main)
- `silver_schema`: Silver schema name (default: silver)
- `gold_schema`: Gold schema name (default: gold)
- `cost_per_call`: Cost per call in dollars (default: 8.0)

### 04 - Run Pipeline

**Purpose**: Orchestrate the complete medallion architecture pipeline

**Pipeline Flow**:
1. Generate dummy data (optional)
2. Bronze layer ingestion
3. Silver layer transformation
4. Gold layer aggregations

**Features**:
- Sequential execution with error handling
- Logging and monitoring
- Performance metrics
- Data quality validation
- Business metrics summary

**Parameters**:
- `run_data_generator`: Whether to generate data (Yes/No, default: Yes)
- `catalog_name`: Unity Catalog name (default: main)
- `bronze_schema`: Bronze schema name (default: bronze)
- `silver_schema`: Silver schema name (default: silver)
- `gold_schema`: Gold schema name (default: gold)
- `input_path`: Path to input data (default: `/dbfs/FileStore/demo_data/sample`)
- `num_members`: Number of members if generating data (default: 10000)
- `num_digital_journeys`: Number of journeys if generating (default: 50000)
- `num_calls`: Number of calls if generating (default: 12000)

## Quick Start

### Option 1: Run Complete Pipeline

```python
# Run the orchestration notebook
%run ./04_run_pipeline
```

This will execute all notebooks in sequence with default parameters.

### Option 2: Run Individual Notebooks

```python
# 1. Generate data
%run ./00_generate_dummy_data

# 2. Bronze ingestion
%run ./01_bronze_ingestion

# 3. Silver transformation
%run ./02_silver_transformation

# 4. Gold aggregations
%run ./03_gold_aggregations
```

### Option 3: Use Databricks Jobs

Create a Databricks Job with these notebook tasks in order:
1. `00_generate_dummy_data` (optional, for initial setup)
2. `01_bronze_ingestion`
3. `02_silver_transformation`
4. `03_gold_aggregations`

Schedule the job to run daily.

## Sample Queries

### Executive Overview
```sql
SELECT 
    leakage_type as journey,
    SUM(daily_calls) as daily_calls,
    SUM(daily_cost) as daily_cost,
    MAX(segment) as primary_segment,
    MAX(trend_pct) as trend
FROM main.gold.daily_leakage_summary
GROUP BY leakage_type
ORDER BY daily_calls DESC
```

### Segment Drill-Down
```sql
SELECT 
    segment,
    daily_calls,
    daily_cost,
    repeat_call_pct,
    avg_sentiment
FROM main.gold.segment_drilldown
WHERE leakage_type = 'billing'
ORDER BY daily_calls DESC
```

### Financial Impact
```sql
SELECT 
    leakage_type,
    avoidable_calls_per_day,
    annualized_cost,
    fix_rate_10_pct_savings as savings_at_10pct,
    fix_rate_50_pct_savings as savings_at_50pct
FROM main.gold.savings_model
ORDER BY annualized_cost DESC
```

### Top Actions
```sql
SELECT 
    priority_rank,
    action_title,
    owner,
    estimated_savings_annual,
    effort_estimate,
    cx_impact_score
FROM main.gold.action_recommendations
ORDER BY priority_rank
```

## Dashboard Views

The Gold layer tables support creating the "Digital to Call Leakage Integrated Command Center" dashboard with these views:

1. **Executive Overview** - High-level leakage ranking with trends
2. **Segment Drill-Down** - Detailed breakdown by member segment
3. **Journey Playback Timeline** - Individual member journey visualization
4. **Financial Savings Model** - Interactive savings projections
5. **Root Cause Analysis** - Error codes and device breakdown
6. **Next-Best Action Recommendations** - Prioritized action list
7. **Before/After Impact Tracker** - Monitor improvement over time

## Technical Requirements

- **Databricks Runtime**: Compatible with DBR 11.3 LTS or higher
- **Python Libraries**: faker (for data generation)
- **Unity Catalog**: Required for table management
- **Delta Lake**: All tables use Delta format
- **Compute**: Recommend 2-4 worker nodes for processing

## Data Volumes

- **Bronze**: ~72,000 raw records
- **Silver**: ~72,000 cleaned records + ~12,000 leakage records
- **Gold**: ~30-50 aggregated records per table (6 tables)

## Performance Optimization

- All tables use Delta format for ACID transactions
- Partitioning can be added for production workloads
- Z-ordering recommended on frequently queried columns
- Caching strategies for dashboard tables

## Next Steps

1. **Dashboard Creation**
   - Use Databricks SQL or Lakeview
   - Connect to Gold layer tables
   - Create visualizations per dashboard design

2. **Genie Integration**
   - Enable Genie on Gold schema
   - Test natural language queries
   - Configure for executive self-service

3. **Agent Framework**
   - Build Insight Scout agent for spike detection
   - Create Next-Best-Action agent
   - Integrate with alerting system

4. **Incremental Processing**
   - Modify for incremental updates
   - Use Delta Lake change data feed
   - Optimize for streaming ingestion

5. **Data Quality Monitoring**
   - Set up data quality expectations
   - Monitor metrics over time
   - Alert on anomalies

## Support

For questions or issues, refer to the main README.md file in the repository root.
