# Delta Live Table Pipeline with Medallion Architecture - Enhanced Version
# This file demonstrates an optimized medallion architecture implementation using Delta Live Tables
# with comprehensive data quality controls, performance optimizations, and best practices

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame

# Import the configuration system
from pipeline_config import get_config

# Get environment-specific configuration
config = get_config()  # Reads from environment variable PIPELINE_ENV or defaults to 'dev'

# Build schema from configuration
def build_schema():
    """Build StructType schema from configuration"""
    fields = []
    schema_columns = [
        ("order_id", "STRING", False),
        ("customer_id", "STRING", False), 
        ("order_date", "STRING", True),
        ("product_id", "STRING", False),
        ("product_name", "STRING", True),
        ("category", "STRING", True),
        ("quantity", "INT", True),
        ("price", "DOUBLE", True),
        ("status", "STRING", True)
    ]
    
    for col_name, col_type, nullable in schema_columns:
        if col_type == "STRING":
            spark_type = StringType()
        elif col_type == "INT":
            spark_type = IntegerType()
        elif col_type == "DOUBLE":
            spark_type = DoubleType()
        else:
            raise ValueError(f"Unsupported column type: {col_type}")
        
        fields.append(StructField(col_name, spark_type, nullable))
    
    return StructType(fields)

order_schema = build_schema()

# BRONZE LAYER - Raw Data Ingestion with Enhanced Configuration
# The bronze layer ingests raw data with minimal transformations but better optimization
@dlt.table(
    name=config.get_table_name("bronze", "orders"),
    comment="Raw e-commerce orders ingested from JSON files with optimized configuration",
    table_properties=config.get_bronze_table_properties(["order_id", "order_date"])
)
def bronze_orders():
    streaming_options = config.get_streaming_options()
    
    return (
        spark.readStream
        .format("json")
        .schema(order_schema)
        .options(**streaming_options)  # Apply all streaming options from config
        .load(config.orders_source_path)  # Use configured source path
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))  # Fixed for Unity Catalog
    )

# SILVER LAYER - Comprehensive Data Cleaning and Validation
# Enhanced with more robust data quality expectations and better transformations
@dlt.table(
    name=config.get_table_name("silver", "orders"),
    comment="Cleaned and validated orders with comprehensive data quality controls",
    table_properties=config.get_silver_table_properties(["order_id", "customer_id", "order_date"]),
    partition_cols=["order_date"]  # Partition for better query performance
)
# Enhanced data quality expectations with configuration values
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL AND length(trim(order_id)) > 0")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL AND length(trim(customer_id)) > 0")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND length(trim(product_id)) > 0")
@dlt.expect_or_drop("valid_order_date", "order_date IS NOT NULL AND length(order_date) = 10")
@dlt.expect_or_drop("valid_quantity", f"quantity > 0 AND quantity <= {config.max_quantity}")
@dlt.expect_or_drop("valid_price", f"price > 0 AND price <= {config.max_price}")
@dlt.expect_or_drop("valid_order_date_format", "order_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .withWatermark("ingestion_timestamp", config.watermark_delay)  # Use configured watermark
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("status", coalesce(col("status"), lit("unknown")))
        .withColumn("category", coalesce(col("category"), lit("uncategorized")))
        .withColumn("product_name", coalesce(col("product_name"), lit("unknown")))
        .withColumn("total_item_price", round(col("price") * col("quantity"), 2))
        .withColumn("processed_timestamp", current_timestamp())
        .withColumn("order_year", year(col("order_date")))
        .withColumn("order_month", month(col("order_date")))
        .withColumn("order_quarter", quarter(col("order_date")))
        .withColumn("order_day_of_week", dayofweek(col("order_date")))
        .withColumn("price_tier", 
            when(col("price") <= config.low_price_threshold, "Low")
            .when(col("price") <= config.medium_price_threshold, "Medium")
            .otherwise("High")
        )
        .dropDuplicates(["order_id", "product_id"])  # Remove duplicates at grain level
        .select(
            "order_id", "customer_id", "order_date", "product_id", "product_name",
            "category", "quantity", "price", "status", "total_item_price",
            "processed_timestamp", "order_year", "order_month", "order_quarter",
            "order_day_of_week", "price_tier", "source_file"
        )
    )

# Enhanced customer dimension with SCD Type 1 logic
@dlt.table(
    name=config.get_table_name("silver", "customers"), 
    comment="Customer dimension with first and latest order tracking",
    table_properties=config.get_silver_table_properties(["customer_id"])
)
@dlt.expect_or_drop("valid_customer_record", "customer_id IS NOT NULL")
def silver_customers():
    return (
        dlt.read("silver_orders")
        .groupBy("customer_id")
        .agg(
            min("order_date").alias("first_order_date"),
            max("order_date").alias("latest_order_date"),
            countDistinct("order_id").alias("total_orders"),
            sum("total_item_price").alias("lifetime_value"),
            count("*").alias("total_items_purchased")
        )
        .withColumn("customer_tenure_days", 
            datediff(col("latest_order_date"), col("first_order_date")))
        .withColumn("customer_segment",
            when(col("lifetime_value") >= config.premium_customer_threshold, "Premium")
            .when(col("lifetime_value") >= config.standard_customer_threshold, "Standard")
            .otherwise("Basic")
        )
        .withColumn("updated_timestamp", current_timestamp())
    )

# GOLD LAYER - Business-Level Aggregates with Advanced Analytics
# Enhanced with more comprehensive metrics and time-based partitioning

@dlt.table(
    name=config.get_table_name("gold", "daily_sales"),
    comment="Daily sales aggregates by category with advanced metrics",
    table_properties=config.get_gold_table_properties(["order_date", "category"]),
    partition_cols=["order_year", "order_month"]
)
def gold_daily_sales():
    return (
        dlt.read("silver_orders")
        .groupBy("order_date", "category", "order_year", "order_month")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            sum("quantity").alias("total_items_sold"),
            sum("total_item_price").alias("total_revenue"),
            avg("total_item_price").alias("avg_order_value"),
            min("total_item_price").alias("min_order_value"),
            max("total_item_price").alias("max_order_value"),
            stddev("total_item_price").alias("order_value_stddev"),
            avg("quantity").alias("avg_items_per_order")
        )
        .withColumn("revenue_per_customer", 
            round(col("total_revenue") / col("unique_customers"), 2))
        .withColumn("items_per_customer",
            round(col("total_items_sold") / col("unique_customers"), 2))
        .withColumn("calculated_timestamp", current_timestamp())
        .orderBy("order_date", "category")
    )

@dlt.table(
    name=config.get_table_name("gold", "customer_insights"),
    comment="Comprehensive customer analytics with behavioral segmentation",
    table_properties=config.get_gold_table_properties(["customer_id", "customer_segment"])
)
def gold_customer_insights():
    return (
        dlt.read("silver_orders")
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            countDistinct("product_id").alias("unique_products_purchased"),
            countDistinct("category").alias("categories_shopped"),
            sum("total_item_price").alias("lifetime_value"),
            avg("total_item_price").alias("avg_order_value"),
            sum("quantity").alias("total_items_purchased"),
            max("order_date").alias("latest_order_date"),
            min("order_date").alias("first_order_date"),
            count("*").alias("total_line_items")
        )
        .withColumn("customer_tenure_days", 
            datediff(col("latest_order_date"), col("first_order_date")))
        .withColumn("days_since_last_order",
            datediff(current_date(), col("latest_order_date")))
        .withColumn("avg_days_between_orders",
            when(col("total_orders") > 1, 
                col("customer_tenure_days") / (col("total_orders") - 1))
            .otherwise(lit(None)))
        # Use configuration values for segmentation
        .withColumn("customer_frequency_segment",
            when(col("total_orders") >= config.frequent_customer_orders, "Frequent")
            .when(col("total_orders") >= config.regular_customer_orders, "Regular") 
            .when(col("total_orders") >= config.occasional_customer_orders, "Occasional")
            .otherwise("One-time"))
        .withColumn("customer_value_segment",
            when(col("lifetime_value") >= config.premium_customer_threshold, "High Value")
            .when(col("lifetime_value") >= config.standard_customer_threshold, "Medium Value")
            .otherwise("Low Value"))
        .withColumn("customer_recency_segment",
            when(col("days_since_last_order") <= config.recent_days_threshold, "Recent")
            .when(col("days_since_last_order") <= config.moderately_recent_days_threshold, "Moderately Recent")
            .when(col("days_since_last_order") <= config.at_risk_days_threshold, "At Risk")
            .otherwise("Dormant"))
        .withColumn("calculated_timestamp", current_timestamp())
    )

@dlt.table(
    name=config.get_table_name("gold", "product_performance"),
    comment="Product performance metrics with trend analysis",
    table_properties=config.get_gold_table_properties(["category", "total_revenue"])
)
def gold_product_performance():
    return (
        dlt.read("silver_orders")
        .groupBy("product_id", "product_name", "category")
        .agg(
            sum("quantity").alias("total_units_sold"),
            sum("total_item_price").alias("total_revenue"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("order_id").alias("unique_orders"),
            avg("price").alias("avg_price"),
            min("price").alias("min_price"),
            max("price").alias("max_price"),
            avg("quantity").alias("avg_quantity_per_order"),
            max("order_date").alias("latest_order_date"),
            min("order_date").alias("first_order_date")
        )
        .withColumn("revenue_per_customer",
            round(col("total_revenue") / col("unique_customers"), 2))
        .withColumn("product_popularity_score",
            round((col("unique_customers") * 0.4 + col("total_units_sold") * 0.6) / 100, 2))
        .withColumn("price_stability",
            when(col("min_price") == col("max_price"), "Stable")
            .when((col("max_price") - col("min_price")) / col("avg_price") <= 0.1, "Low Variance")
            .when((col("max_price") - col("min_price")) / col("avg_price") <= 0.3, "Medium Variance")
            .otherwise("High Variance"))
        .withColumn("product_lifecycle_days",
            datediff(col("latest_order_date"), col("first_order_date")))
        .withColumn("calculated_timestamp", current_timestamp())
        .orderBy(col("total_revenue").desc())
    )

# Real-time analytics table for operational reporting
@dlt.table(
    name=config.get_table_name("gold", "real_time_metrics"),
    comment="Real-time operational metrics and KPIs",
    table_properties=config.get_gold_table_properties(["metric_date"])
)
def gold_real_time_metrics():
    orders_df = dlt.read("silver_orders")
    
    # Calculate today's metrics
    today_metrics = (
        orders_df.filter(col("order_date") == current_date())
        .agg(
            count("*").alias("todays_orders"),
            sum("total_item_price").alias("todays_revenue"),
            countDistinct("customer_id").alias("todays_customers"),
            avg("total_item_price").alias("todays_avg_order_value")
        )
        .withColumn("metric_date", current_date())
        .withColumn("metric_type", lit("daily"))
    )
    
    # Calculate last 7 days rolling metrics
    last_7_days = (
        orders_df.filter(col("order_date") >= date_sub(current_date(), 7))
        .agg(
            count("*").alias("orders_7d"),
            sum("total_item_price").alias("revenue_7d"),
            countDistinct("customer_id").alias("customers_7d"),
            avg("total_item_price").alias("avg_order_value_7d")
        )
        .withColumn("metric_date", current_date())
        .withColumn("metric_type", lit("7_day_rolling"))
    )
    
    return today_metrics.unionByName(last_7_days, allowMissingColumns=True)

# Configuration logging for debugging
def log_pipeline_config():
    """Log current configuration for debugging"""
    print(f"=== Pipeline Configuration ===")
    print(f"Environment: {config.environment}")
    print(f"Catalog.Schema: {config.catalog}.{config.schema}")
    print(f"Source Path: {config.orders_source_path}")
    print(f"Max Files per Trigger: {config.max_files_per_trigger}")
    print(f"Target File Size: {config.target_file_size_mb}MB")
    print(f"Watermark Delay: {config.watermark_delay}")
    print(f"Premium Customer Threshold: ${config.premium_customer_threshold}")
    print(f"Max Quantity: {config.max_quantity}")
    print(f"Max Price: ${config.max_price}")
    print(f"==============================")

# Call configuration logging at pipeline start
log_pipeline_config() 