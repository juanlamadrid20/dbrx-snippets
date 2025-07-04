# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks Delta Live Tables project demonstrating a medallion architecture (Bronze → Silver → Gold) for e-commerce data processing. The project uses Databricks Asset Bundles for deployment and includes ML feature engineering capabilities.

## Key Commands

### Databricks Asset Bundle Commands
```bash
# Validate bundle configuration
databricks bundle validate

# Deploy to development environment
databricks bundle deploy
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t prod

# Run data generation job (generates both order and customer activity data)
databricks bundle run -t dev generate_sample_data

# Run individual pipelines
databricks bundle run medallion_dlt_pipeline  # Order processing pipeline
databricks bundle run ml_features_pipeline    # ML features pipeline

# Run the complete end-to-end pipeline job
databricks bundle run -t dev e2e_pipeline_job
```

### Python Environment
```bash
# Install dependencies (uses uv for fast dependency resolution)
uv sync

# Run Python scripts
python de/pipeline_main.py
```

## Architecture

### Core Components

1. **Delta Live Tables Pipeline** (`de/pipeline_main.py`):
   - Implements medallion architecture with Bronze → Silver → Gold layers
   - Uses streaming ingestion with configurable batch sizes
   - Includes comprehensive data quality expectations
   - Supports environment-specific configurations

2. **Configuration System** (`de/pipeline_config.py`):
   - Centralized configuration with environment-specific settings (dev/staging/prod)
   - Configurable table properties, streaming options, and data quality thresholds
   - Environment detection via `PIPELINE_ENV` environment variable

3. **Feature Engineering** (`de/pipeline_features.py`):
   - ML-ready feature tables for customer behavior analysis
   - Demonstrates feature engineering patterns for recommendation systems

4. **Data Generation** (`de/data_gen_job.py`):
   - Generates comprehensive synthetic data: orders, customer activity, and profiles
   - Creates realistic customer behavior patterns with intentional quality issues
   - Supports both order processing and ML feature pipelines

### Data Architecture

- **Bronze Layer**: Raw JSON ingestion with minimal transformation
- **Silver Layer**: Cleaned, validated data with business rules applied
- **Gold Layer**: Business-ready aggregates and analytics tables
- **Feature Layer**: ML-ready features for recommendation and prediction models

### Key Patterns

1. **Environment-Specific Configuration**:
   ```python
   from pipeline_config import get_config
   
   # Configuration is environment-aware
   config = get_config()  # Reads PIPELINE_ENV or defaults to 'dev'
   
   # Or explicitly specify environment
   dev_config = get_config('dev')
   staging_config = get_config('staging')
   prod_config = get_config('prod')
   ```

2. **Table Name Generation**:
   ```python
   # Generates: "juan_dev.data_eng.bronze_orders"
   bronze_table = config.get_table_name("bronze", "orders")
   
   # Generates: "juan_dev.data_eng.silver_customers"  
   silver_table = config.get_table_name("silver", "customers")
   
   # Generates: "juan_dev.data_eng.gold_daily_sales"
   gold_table = config.get_table_name("gold", "daily_sales")
   ```

3. **Table Properties from Configuration**:
   ```python
   # Bronze layer properties
   bronze_props = config.get_bronze_table_properties(["order_id", "order_date"])
   
   # Silver layer properties
   silver_props = config.get_silver_table_properties(["customer_id", "order_date"])
   
   # Gold layer properties  
   gold_props = config.get_gold_table_properties(["metric_date"])
   ```

4. **Streaming Configuration**:
   ```python
   # Get all streaming configuration options
   streaming_opts = config.get_streaming_options()
   
   # Apply to readStream
   df = (spark.readStream
         .format("json")
         .options(**streaming_opts)  # Applies all options at once
         .load(config.orders_source_path))
   ```

5. **Data Quality Expectations**:
   - Uses `@dlt.expect_or_drop` decorators for data quality
   - Configurable thresholds for business rules
   - Comprehensive validation at each layer

6. **Performance Optimization**:
   - Z-ordering on frequently queried columns
   - Auto-optimization enabled with configurable file sizes
   - Streaming watermarks for late-arriving data

### Storage Configuration

- **Data Location**: `/Volumes/juan_dev/data_eng/data`
  - `orders_raw/`: E-commerce order data (for main pipeline)
  - `customer_activity_raw/`: Customer session and interaction data (for ML features)
  - `customer_profiles_raw/`: Customer demographic and profile data (for ML features)
- **Catalog**: `juan_dev` (development), configurable per environment
- **Schema**: `data_eng`

### Table Naming Convention

Tables follow the pattern: `{catalog}.{schema}.{layer}_{entity}`
- Example: `juan_dev.data_eng.silver_orders`

## Environment Variables

- `PIPELINE_ENV`: Controls which configuration environment to use (dev/staging/prod)

## Important Files

- `databricks.yml`: Asset bundle configuration defining pipelines, jobs, and deployment targets
- `de/pipeline_config.py`: Centralized configuration system with environment-specific settings
- `de/pipeline_main.py`: Main DLT pipeline implementation
- `de/pipeline_features.py`: ML feature engineering examples
- `de/data_gen_job.py`: Synthetic data generation for testing

## Testing Approach

The project uses comprehensive synthetic data generation (`de/data_gen_job.py`) that creates:

### Generated Datasets
1. **E-commerce Orders**: Product purchases with pricing, quantities, and order status
2. **Customer Activity**: Realistic session data with page views, cart adds, and device patterns
3. **Customer Profiles**: Demographics, signup dates, and loyalty information

### Data Quality Testing
- Intentional data quality issues (5% of records) including null values and invalid formats
- Realistic distributions using exponential and normal patterns
- Cross-dataset consistency (customer IDs match across all datasets)

### ML Features Testing
The generated data supports comprehensive ML feature engineering including:
- Customer engagement scoring and segmentation
- Device preference and multi-device usage patterns
- Churn risk assessment based on activity patterns
- Customer lifetime value and purchase behavior analysis

## Performance Optimization

### Delta Lake Features
- **Auto-Optimization**: Enabled `delta.autoOptimize.optimizeWrite` and `delta.autoOptimize.autoCompact`
- **Target File Size**: Configured to 128MB for optimal read performance
- **Z-Ordering**: Strategic Z-ordering columns for data skipping optimization
- **Change Data Feed**: Enabled for tracking data changes over time
- **Deletion Vectors**: Enabled for faster UPDATE/DELETE operations

### Streaming Configuration
- **Batch Control**: `maxFilesPerTrigger` (dev: 100, staging: 500, prod: 1000)
- **Memory Management**: `maxBytesPerTrigger` (1GB per batch)
- **Watermarking**: 10-minute watermark for handling late-arriving data
- **Schema Evolution**: Controlled schema evolution with Auto Loader

### Environment-Specific Settings
| Setting | Dev | Staging | Prod |
|---------|-----|---------|------|
| Files per trigger | 100 | 500 | 1000 |
| Target file size | 64MB | 128MB | 256MB |
| Log retention | 60 days | 60 days | 90 days |
| Catalog | `juan_dev` | `staging` | `prod` |

## Data Quality Framework

### Multi-Level Validation
1. **Bronze Layer**: Schema validation and basic constraints
2. **Silver Layer**: Comprehensive business rule validation
3. **Gold Layer**: Analytical consistency checks

### Example Expectations
```python
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL AND length(trim(order_id)) > 0")
@dlt.expect_or_drop("valid_quantity", f"quantity > 0 AND quantity <= {config.max_quantity}")
@dlt.expect_or_drop("valid_price", f"price > 0 AND price <= {config.max_price}")
@dlt.expect_or_fail("critical_data_quality", "count(*) > 0")
```

## Monitoring and Observability

### Key Metrics to Monitor
```sql
-- Monitor pipeline run status
SELECT 
  pipeline_id, update_id, state, start_time, end_time,
  TIMESTAMPDIFF(MINUTE, start_time, end_time) as duration_minutes
FROM event_log('your_pipeline_id')
WHERE event_type = 'update'
ORDER BY start_time DESC;

-- Monitor data quality expectation failures
SELECT 
  timestamp, table_name, expectation_name, failed_records, total_records,
  (failed_records / total_records) * 100 as failure_rate_pct
FROM event_log('your_pipeline_id')
WHERE event_type = 'expectation' AND level = 'WARN'
ORDER BY timestamp DESC;
```

### Pipeline Health KPIs
- **Uptime**: > 99.5% successful pipeline runs
- **Data Quality**: < 1% expectation failure rate
- **Latency**: End-to-end processing time within SLA
- **Cost Efficiency**: Cost per GB processed trending down

## Maintenance Operations

### Regular Schedule
- **Daily**: Monitor pipeline execution, check data quality metrics
- **Weekly**: Review Z-ordering, analyze performance, update expectations
- **Monthly**: Comprehensive review, schema evolution planning, cost optimization

### Vacuum Operations
```sql
-- Schedule VACUUM operations (during low-usage periods)
VACUUM your_catalog.your_schema.silver_orders RETAIN 168 HOURS; -- 7 days
```

### Performance Optimization
```sql
-- Optimize with Z-ordering based on query patterns
OPTIMIZE your_catalog.your_schema.silver_orders
ZORDER BY (order_date, customer_id, category);
```

## Troubleshooting Common Issues

### Pipeline Failures
1. Check event logs for error details
2. Monitor resource usage (memory, CPU)
3. Verify source data accessibility
4. Validate configuration settings

### Data Quality Issues
1. **High Failure Rates**: Review source data changes, adjust expectations
2. **Schema Mismatches**: Enable schema evolution, update expectations
3. **Performance Issues**: Check file sizes, review Z-ordering

### Configuration Issues
```bash
# Check environment variable
echo $PIPELINE_ENV

# Valid values: dev, staging, prod
export PIPELINE_ENV=dev
```

## Advanced Analytics Features

### Customer Intelligence
- **RFM Analysis**: Recency, Frequency, Monetary segmentation
- **Behavioral Segmentation**: Customer lifecycle and value tiers
- **Predictive Segmentation**: Risk analysis for customer retention

### Product Performance
- **Popularity Scoring**: Weighted algorithm combining customer reach and volume
- **Price Intelligence**: Stability and variance analysis
- **Lifecycle Tracking**: Product performance over time

### Real-time Metrics
- **Operational KPIs**: Daily and rolling 7-day metrics
- **Performance Monitoring**: Real-time pipeline health indicators

## Configuration Usage Examples

### Environment-Aware DLT Table
```python
import dlt
from pipeline_config import get_config

config = get_config()

@dlt.table(
    name=config.get_table_name("bronze", "orders"),
    table_properties=config.get_bronze_table_properties(["order_id"]),
    comment=f"Bronze orders table for {config.environment} environment"
)
def bronze_orders():
    return (
        spark.readStream
        .format("json")
        .options(**config.get_streaming_options())
        .load(config.orders_source_path)
        .withColumn("environment", lit(config.environment))
    )
```

### Business Logic Using Configuration
```python
# Customer segmentation using configuration thresholds
def segment_customers(df):
    return (
        df.withColumn("customer_segment",
            when(col("lifetime_value") >= config.premium_customer_threshold, "Premium")
            .when(col("lifetime_value") >= config.standard_customer_threshold, "Standard")
            .otherwise("Basic")
        )
        .withColumn("frequency_segment",
            when(col("total_orders") >= config.frequent_customer_orders, "Frequent")
            .when(col("total_orders") >= config.regular_customer_orders, "Regular")
            .otherwise("Occasional")
        )
    )
```

### Data Quality with Configuration
```python
# Use configuration values in expectations
@dlt.expect_or_drop("valid_quantity", f"quantity > 0 AND quantity <= {config.max_quantity}")
@dlt.expect_or_drop("valid_price", f"price > 0 AND price <= {config.max_price}")

# Use business thresholds
@dlt.expect("reasonable_order_value", 
           f"total_order_value <= {config.max_price * config.max_quantity}")
```

### Databricks Deployment Options

#### Option 1: Environment Variables in Job Configuration
```json
{
  "name": "dlt-pipeline-dev",
  "environment_variables": {
    "PIPELINE_ENV": "dev"
  },
  "libraries": [...],
  "pipeline_spec": {...}
}
```

#### Option 2: Widget-Based Configuration
```python
# At the top of your notebook
dbutils.widgets.text("environment", "dev", "Environment (dev/staging/prod)")
env = dbutils.widgets.get("environment")

config = get_config(env)
```

#### Option 3: Delta Live Tables Settings
```json
{
  "configuration": {
    "PIPELINE_ENV": "prod"
  }
}
```

### Configuration Best Practices

1. **Always use environment variables** in production deployments
2. **Test configuration changes** in dev/staging first
3. **Monitor resource usage** and adjust batch sizes accordingly
4. **Use consistent naming** through `get_table_name()`
5. **Centralize business logic** in configuration rather than hardcoding
6. **Version your configuration** changes alongside code changes

### Configuration Debugging
```python
def log_pipeline_config():
    """Log current configuration for debugging"""
    config = get_config()
    
    print(f"Pipeline Environment: {config.environment}")
    print(f"Target Catalog.Schema: {config.catalog}.{config.schema}")
    print(f"Source Path: {config.orders_source_path}")
    print(f"Max Files per Trigger: {config.max_files_per_trigger}")
    print(f"Target File Size: {config.target_file_size_mb}MB")
    print(f"Watermark Delay: {config.watermark_delay}")
    
    # Log business thresholds
    print(f"Premium Customer Threshold: ${config.premium_customer_threshold}")
    print(f"Max Quantity Allowed: {config.max_quantity}")
    print(f"Max Price Allowed: ${config.max_price}")

# Call at pipeline start
log_pipeline_config()
```