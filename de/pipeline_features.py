# Feature Table Implementation for ML Features
# This file demonstrates how to create and manage feature tables for machine learning

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

# Import configuration for environment-aware paths
from pipeline_config import get_config

# Get configuration
config = get_config()

# Define schema for customer activity data
customer_activity_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("page_views", IntegerType(), True),
    StructField("cart_adds", IntegerType(), True),
    StructField("purchases", IntegerType(), True),
    StructField("device", StringType(), True),
    StructField("location", StringType(), True),
    StructField("session_id", StringType(), True)
])

# BRONZE LAYER
# Ingest raw customer activity data from generated source
@dlt.table(
    name=config.get_table_name("bronze", "customer_activity"),
    comment="Raw customer activity logs ingested from generated JSON files",
    table_properties=config.get_bronze_table_properties(["customer_id", "timestamp"])
)
def bronze_customer_activity():
    activity_source_path = f"{config.source_base_path}/customer_activity_raw"
    
    return (
        spark.readStream
        .format("json")
        .schema(customer_activity_schema)
        .options(**config.get_streaming_options())
        .load(activity_source_path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )

# SILVER LAYER
# Clean and prepare the activity data
@dlt.table(
    name=config.get_table_name("silver", "customer_activity"),
    comment="Cleaned and enriched customer activity data with streaming processing",
    table_properties=config.get_silver_table_properties(["customer_id", "activity_date"]),
    partition_cols=["activity_date"]
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL AND timestamp != 'invalid-timestamp'")
@dlt.expect_or_drop("valid_page_views", "page_views >= 0")
@dlt.expect_or_drop("valid_cart_adds", "cart_adds >= 0")
@dlt.expect_or_drop("valid_purchases", "purchases >= 0")
def silver_customer_activity():
    return (
        dlt.read_stream("bronze_customer_activity")
        .withWatermark("ingestion_timestamp", config.watermark_delay)
        .withColumn("activity_date", to_date(col("timestamp")))
        .withColumn("activity_hour", hour(col("timestamp")))
        .withColumn("is_weekend", dayofweek(col("activity_date")).isin(1, 7))
        .withColumn("engagement_score", 
                    col("page_views") * 1 + 
                    col("cart_adds") * 5 + 
                    col("purchases") * 10)
        .withColumn("processed_timestamp", current_timestamp())
        .withColumn("activity_year", year(col("activity_date")))
        .withColumn("activity_month", month(col("activity_date")))
        .dropDuplicates(["customer_id", "session_id"])  # Remove duplicate sessions
    )

# Define schema for customer profiles
customer_profile_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("signup_date", StringType(), True),
    StructField("age_group", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("income_bracket", StringType(), True),
    StructField("loyalty_tier", StringType(), True)
])

@dlt.table(
    name=config.get_table_name("bronze", "customer_profiles"),
    comment="Raw customer profile data ingested from generated JSON files",
    table_properties=config.get_bronze_table_properties(["customer_id"])
)
def bronze_customer_profiles():
    profile_source_path = f"{config.source_base_path}/customer_profiles_raw"
    
    return (
        spark.read
        .format("json")
        .schema(customer_profile_schema)
        .load(profile_source_path)
        .withColumn("ingestion_timestamp", current_timestamp())
    )

@dlt.table(
    name=config.get_table_name("silver", "customer_profile"),
    comment="Cleaned customer profile information with demographics",
    table_properties=config.get_silver_table_properties(["customer_id"])
)
@dlt.expect_or_drop("valid_customer_profile_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_signup_date", "signup_date IS NOT NULL")
def silver_customer_profile():
    return (
        dlt.read("bronze_customer_profiles")
        .withColumn("signup_date_parsed", to_date(col("signup_date"), "yyyy-MM-dd"))
        .withColumn("days_since_signup", 
                    datediff(current_date(), col("signup_date_parsed")))
        .withColumn("customer_vintage_segment",
                    when(col("days_since_signup") <= 90, "New")
                    .when(col("days_since_signup") <= 365, "Growing")
                    .when(col("days_since_signup") <= 730, "Established")
                    .otherwise("Veteran"))
        .withColumn("processed_timestamp", current_timestamp())
    )

# FEATURE LAYER
# Create features for machine learning models
@dlt.table(
    name=config.get_table_name("gold", "features_customer_engagement"),
    comment="Customer engagement features for ML models with comprehensive metrics",
    table_properties=config.get_gold_table_properties(["customer_id", "last_activity_date"])
)
def features_customer_engagement():
    return (
        dlt.read("silver_customer_activity")
        .groupBy("customer_id")
        .agg(
            sum("page_views").alias("total_page_views"),
            sum("cart_adds").alias("total_cart_adds"),
            sum("purchases").alias("total_purchases"),
            avg("engagement_score").alias("avg_engagement_score"),
            max("engagement_score").alias("max_engagement_score"),
            stddev("engagement_score").alias("engagement_score_stddev"),
            max("activity_date").alias("last_activity_date"),
            min("activity_date").alias("first_activity_date"),
            countDistinct("activity_date").alias("active_days"),
            countDistinct("session_id").alias("total_sessions"),
            approx_count_distinct(
                when(col("is_weekend") == True, col("activity_date"))
            ).alias("weekend_activity_days"),
            avg("activity_hour").alias("avg_activity_hour")
        )
        .withColumn("avg_page_views_per_session", 
                    round(col("total_page_views") / col("total_sessions"), 2))
        .withColumn("conversion_rate_cart_to_purchase",
                    when(col("total_cart_adds") > 0, 
                         round(col("total_purchases") / col("total_cart_adds"), 3))
                    .otherwise(0.0))
        .withColumn("days_since_last_activity",
                    datediff(current_date(), col("last_activity_date")))
        .withColumn("activity_consistency_score",
                    round(col("active_days") / 
                          datediff(col("last_activity_date"), col("first_activity_date")) * 100, 2))
    )

@dlt.table(
    name=config.get_table_name("gold", "features_customer_behavior"),
    comment="Customer behavior patterns and device preferences for ML models",
    table_properties=config.get_gold_table_properties(["customer_id"])
)
def features_customer_behavior():
    # Device usage patterns
    device_features = (
        dlt.read("silver_customer_activity")
        .groupBy("customer_id", "device")
        .agg(
            count("*").alias("session_count"),
            sum("page_views").alias("device_page_views"),
            sum("purchases").alias("device_purchases")
        )
        .groupBy("customer_id")
        .pivot("device")
        .agg(
            first("session_count").alias("sessions"),
            first("device_page_views").alias("page_views"),
            first("device_purchases").alias("purchases")
        )
        .fillna(0)
    )
    
    # Location diversity
    location_features = (
        dlt.read("silver_customer_activity")
        .groupBy("customer_id")
        .agg(
            countDistinct("location").alias("unique_locations"),
            collect_set("location").alias("locations_visited")
        )
    )
    
    return (
        device_features
        .join(location_features, "customer_id", "inner")
        .withColumn("total_sessions", 
                    coalesce(col("desktop_sessions"), lit(0)) + 
                    coalesce(col("mobile_sessions"), lit(0)) + 
                    coalesce(col("tablet_sessions"), lit(0)))
        .withColumn("mobile_preference_score",
                    when(col("total_sessions") > 0,
                         round(coalesce(col("mobile_sessions"), lit(0)) / col("total_sessions"), 3))
                    .otherwise(0.0))
        .withColumn("desktop_preference_score",
                    when(col("total_sessions") > 0,
                         round(coalesce(col("desktop_sessions"), lit(0)) / col("total_sessions"), 3))
                    .otherwise(0.0))
        .withColumn("multi_device_user",
                    when((coalesce(col("desktop_sessions"), lit(0)) > 0) +
                         (coalesce(col("mobile_sessions"), lit(0)) > 0) +
                         (coalesce(col("tablet_sessions"), lit(0)) > 0) >= 2, True)
                    .otherwise(False))
        .drop("locations_visited")  # Remove array column for simpler ML features
    )

@dlt.table(
    name=config.get_table_name("gold", "ml_feature_table"),
    comment="Comprehensive ML feature table ready for training and inference",
    table_properties=config.get_gold_table_properties(["customer_id", "feature_timestamp"])
)
def ml_feature_table():
    engagement_features = dlt.read("features_customer_engagement")
    behavior_features = dlt.read("features_customer_behavior") 
    customer_profile = dlt.read("silver_customer_profile")
    
    # Join all feature tables
    return (
        engagement_features
        .join(behavior_features, "customer_id", "left")
        .join(customer_profile, "customer_id", "left")
        .withColumn("feature_timestamp", current_timestamp())
        # Enhanced churn risk scoring
        .withColumn("churn_risk_score", 
                    when((col("days_since_last_activity") > 30) | 
                         (col("avg_engagement_score") < 3), "high")
                    .when((col("days_since_last_activity") > 14) | 
                          (col("avg_engagement_score") < 8), "medium")
                    .otherwise("low"))
        # Customer value segmentation
        .withColumn("customer_value_segment",
                    when(col("total_purchases") >= 20, "high_value")
                    .when(col("total_purchases") >= 10, "medium_value")
                    .when(col("total_purchases") >= 3, "low_value")
                    .otherwise("minimal_value"))
        # Engagement level classification
        .withColumn("engagement_level",
                    when(col("avg_engagement_score") >= 20, "highly_engaged")
                    .when(col("avg_engagement_score") >= 10, "moderately_engaged")
                    .when(col("avg_engagement_score") >= 5, "lightly_engaged")
                    .otherwise("disengaged"))
        .select(
            # Customer identifiers
            "customer_id", "feature_timestamp",
            # Engagement features
            "total_page_views", "total_cart_adds", "total_purchases",
            "avg_engagement_score", "max_engagement_score", "engagement_score_stddev",
            "active_days", "total_sessions", "weekend_activity_days",
            "avg_page_views_per_session", "conversion_rate_cart_to_purchase",
            "days_since_last_activity", "activity_consistency_score", "avg_activity_hour",
            # Behavioral features  
            "desktop_sessions", "mobile_sessions", "tablet_sessions",
            "mobile_preference_score", "desktop_preference_score", 
            "multi_device_user", "unique_locations",
            # Profile features
            "age_group", "gender", "income_bracket", "loyalty_tier",
            "days_since_signup", "customer_vintage_segment",
            # Derived labels/scores
            "churn_risk_score", "customer_value_segment", "engagement_level"
        )
    ) 