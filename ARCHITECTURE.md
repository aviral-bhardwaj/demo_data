# Medallion Architecture - Technical Summary

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    00_GENERATE_DUMMY_DATA.PY                     │
│                                                                   │
│  Generates synthetic data using Faker library:                   │
│  • Digital Journeys: 50,000 events                              │
│  • Call Logs: 12,000 calls                                      │
│  • Member Profiles: 10,000 members                              │
│                                                                   │
│  Outputs: CSV files → /dbfs/FileStore/demo_data/sample/         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    01_BRONZE_INGESTION.PY                        │
│                      (Bronze Layer - Raw)                         │
│                                                                   │
│  Ingests raw CSV files into Delta tables:                        │
│  ✓ bronze.digital_journeys_raw                                   │
│  ✓ bronze.call_logs_raw                                          │
│  ✓ bronze.member_profiles_raw                                    │
│                                                                   │
│  Added metadata: ingestion_timestamp, source_file                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                  02_SILVER_TRANSFORMATION.PY                      │
│                   (Silver Layer - Cleaned)                        │
│                                                                   │
│  Data cleansing & business logic:                                │
│  ✓ silver.digital_journeys_cleaned                               │
│  ✓ silver.call_logs_cleaned                                      │
│  ✓ silver.member_profiles_enriched                               │
│  ✓ silver.digital_call_leakage ← KEY TABLE                       │
│                                                                   │
│  Leakage Detection Logic:                                        │
│  • Digital abandon/error within 30 min → call                    │
│  • Identifies 12K+ leakage events                                │
│  • Calculates time_to_call, is_repeat_call                       │
│  • Enriches with member segment data                             │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   03_GOLD_AGGREGATIONS.PY                         │
│                  (Gold Layer - Dashboard Ready)                   │
│                                                                   │
│  Business aggregations for Command Center Dashboard:             │
│                                                                   │
│  ✓ gold.daily_leakage_summary                                    │
│    → Executive Overview: Journey, Calls, Cost, Segment, Trend    │
│                                                                   │
│  ✓ gold.segment_drilldown                                        │
│    → Segment Analysis: Repeat %, Sentiment, Cost breakdown       │
│                                                                   │
│  ✓ gold.journey_timeline                                         │
│    → Journey Playback: Member event timeline                     │
│                                                                   │
│  ✓ gold.root_cause_analysis                                      │
│    → Root Cause: Error codes, Device breakdown, Trends           │
│                                                                   │
│  ✓ gold.savings_model                                            │
│    → Financial Model: Cost projections, Savings scenarios        │
│                                                                   │
│  ✓ gold.action_recommendations                                   │
│    → Next-Best Actions: Prioritized fixes with ROI               │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   COMMAND CENTER DASHBOARD                        │
│              (Databricks SQL / Lakeview / Genie)                 │
│                                                                   │
│  Executive Views:                                                │
│  • What's breaking? → daily_leakage_summary                      │
│  • Who's impacted? → segment_drilldown                           │
│  • Journey playback → journey_timeline                           │
│  • Why is this happening? → root_cause_analysis                  │
│  • What's the upside? → savings_model                            │
│  • What to do next? → action_recommendations                     │
└─────────────────────────────────────────────────────────────────┘
```

## Pipeline Orchestration

```
┌─────────────────────────────────────────────────────────────────┐
│                    04_RUN_PIPELINE.PY                            │
│                   (Pipeline Orchestrator)                         │
│                                                                   │
│  Sequential execution with:                                      │
│  • Error handling & logging                                      │
│  • Performance metrics                                           │
│  • Data quality validation                                       │
│  • Business metrics summary                                      │
│                                                                   │
│  Configurable parameters via widgets                             │
└─────────────────────────────────────────────────────────────────┘
```

## Key Metrics Calculated

### Executive Overview
| Journey Type | Daily Calls | Daily Cost | Segment      | Trend |
|--------------|-------------|------------|--------------|-------|
| Billing      | 5,000       | $40,000    | MAPD New     | +14%  |
| ID Card      | 3,000       | $24,000    | Spanish      | +8%   |
| Pharmacy     | 4,000       | $32,000    | Mixed        | +3%   |
| **TOTAL**    | **12,000**  | **$112K**  | -            | -     |

### Segment Analysis (Billing)
| Segment        | Calls | Cost     | Repeat % | Sentiment |
|----------------|-------|----------|----------|-----------|
| MAPD New       | 3,000 | $24,000  | 28%      | -0.65     |
| MAPD Tenured   | 1,200 | $9,600   | 18%      | -0.40     |
| Commercial     | 800   | $6,400   | 15%      | -0.30     |

### Financial Savings Model
| Journey | Annual Cost | 10% Fix    | 25% Fix    | 50% Fix    |
|---------|-------------|------------|------------|------------|
| Billing | $14.6M      | $1.46M     | $3.65M     | $7.3M      |
| ID Card | $8.76M      | $876K      | $2.19M     | $4.38M     |
| Pharmacy| $11.68M     | $1.17M     | $2.92M     | $5.84M     |
| **Total**| **$35M**   | **$3.5M**  | **$8.8M**  | **$17.5M** |

### Top Action Recommendations
| Priority | Action                              | Owner      | Savings | Effort    |
|----------|-------------------------------------|------------|---------|-----------|
| 1        | Update MAPD Billing Message         | Digital UX | $1.0M   | 1 sprint  |
| 2        | Proactive IVR Messaging             | Contact Ctr| $300K   | 1 week    |
| 3        | Simplified Agent Script             | Contact Ctr| $160K   | 3 days    |

## Technical Architecture

### Storage Format
- **Format**: Delta Lake
- **Location**: Unity Catalog
- **ACID**: Transactions supported
- **Time Travel**: Versioning enabled

### Data Volumes
- **Bronze**: ~72K records (raw)
- **Silver**: ~84K records (cleaned + leakage)
- **Gold**: ~200 records (aggregated)

### Processing Pattern
- **Pattern**: Batch processing (daily)
- **Window**: 30-minute leakage detection
- **Refresh**: Full refresh (can be incremental)

### Performance Optimizations
- Delta format for fast queries
- Partitioning capability (by date)
- Z-ordering on key columns
- Pre-aggregated Gold tables

## Use Case Alignment

### README Requirements Met ✓
- [x] 5,000 billing abandons → calls (MAPD New, +14%)
- [x] 3,000 ID card failures → calls (Spanish, +8%)
- [x] 4,000 pharmacy issues → calls (Mixed, +3%)
- [x] Cost per call: $8
- [x] MAPD New: 28% repeat rate, -0.65 sentiment
- [x] Total daily cost: ~$112K

### Dashboard PNG Alignment ✓
- [x] Executive Overview with ranking
- [x] Segment Drill-Down tables
- [x] Journey Playback capability
- [x] Financial Savings Model
- [x] Root Cause Analysis
- [x] Next-Best Action Recommendations

## Deployment Guide

### Step 1: Upload to Databricks
```bash
# Upload notebooks to Databricks Workspace
databricks workspace import-dir ./notebooks /Workspace/Repos/demo_data/notebooks
```

### Step 2: Run Pipeline
```python
# Option A: Run orchestrator
%run /Workspace/Repos/demo_data/notebooks/04_run_pipeline

# Option B: Create Databricks Job
# Add notebook tasks in order:
# 1. 00_generate_dummy_data (one-time)
# 2. 01_bronze_ingestion
# 3. 02_silver_transformation
# 4. 03_gold_aggregations
```

### Step 3: Create Dashboard
```sql
-- Connect Databricks SQL to Gold tables
USE CATALOG main;
USE SCHEMA gold;

-- Query example
SELECT * FROM daily_leakage_summary;
```

### Step 4: Enable Genie (Optional)
```
Natural language queries:
"Show me top digital billing failures today"
"What's the cost of pharmacy leakage?"
"Which segment has the worst sentiment?"
```

## Future Enhancements

1. **Streaming Ingestion**
   - Replace batch CSV with streaming sources
   - Real-time leakage detection
   - Live dashboard updates

2. **ML Models**
   - Predict leakage likelihood
   - Sentiment analysis on call transcripts
   - Churn risk scoring

3. **Agent Framework**
   - Automated spike detection
   - Intelligent action recommendations
   - Closed-loop feedback

4. **Advanced Analytics**
   - Cohort analysis
   - Time-series forecasting
   - A/B test impact measurement

## Support & Resources

- **Documentation**: See notebooks/README.md
- **Schemas**: See notebooks/config/schema_definitions.py
- **Dashboard Reference**: Digital to Call Leakage Integrated Command Center.png
- **Use Case Details**: README.md (root)
