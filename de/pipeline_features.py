# Feature Table Implementation for ML Features
# This file demonstrates how to create and manage feature tables for machine learning

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

# BRONZE LAYER
# Ingest raw customer activity data
@dlt.table(
    name="bronze_customer_activity",
    comment="Raw customer activity logs ingested from JSON files",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "customer_id"
    }
)
def bronze_customer_activity():
    # In a real scenario, this would read from an external source
    # For this example, we'll generate synthetic data
    data = [
        {"customer_id": 1001, "timestamp": "2023-10-01T10:15:00", "page_views": 5, "cart_adds": 2, "purchases": 1, "device": "mobile", "location": "New York"},
        {"customer_id": 1002, "timestamp": "2023-10-01T11:20:00", "page_views": 8, "cart_adds": 0, "purchases": 0, "device": "desktop", "location": "San Francisco"},
        {"customer_id": 1003, "timestamp": "2023-10-01T12:30:00", "page_views": 12, "cart_adds": 3, "purchases": 2, "device": "tablet", "location": "Chicago"},
        {"customer_id": 1001, "timestamp": "2023-10-02T09:45:00", "page_views": 3, "cart_adds": 1, "purchases": 1, "device": "mobile", "location": "New York"},
        {"customer_id": 1004, "timestamp": "2023-10-02T14:10:00", "page_views": 7, "cart_adds": 4, "purchases": 0, "device": "desktop", "location": "Austin"},
        {"customer_id": 1002, "timestamp": "2023-10-02T16:25:00", "page_views": 6, "cart_adds": 2, "purchases": 1, "device": "mobile", "location": "San Francisco"},
        {"customer_id": 1005, "timestamp": "2023-10-03T08:30:00", "page_views": 9, "cart_adds": 1, "purchases": 1, "device": "tablet", "location": "Seattle"},
        {"customer_id": 1003, "timestamp": "2023-10-03T13:15:00", "page_views": 4, "cart_adds": 0, "purchases": 0, "device": "desktop", "location": "Chicago"}
    ]
    
    return spark.createDataFrame(data)

# SILVER LAYER
# Clean and prepare the activity data
@dlt.table(
    name="silver_customer_activity",
    comment="Cleaned and enriched customer activity data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "customer_id,activity_date"
    }
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
def silver_customer_activity():
    return (
        dlt.read("bronze_customer_activity")
        .withColumn("activity_date", to_date(col("timestamp")))
        .withColumn("activity_hour", hour(col("timestamp")))
        .withColumn("is_weekend", dayofweek(col("activity_date")).isin(1, 7))
        .withColumn("engagement_score", 
                    col("page_views") * 1 + 
                    col("cart_adds") * 5 + 
                    col("purchases") * 10)
        .withColumn("processed_timestamp", current_timestamp())
    )

@dlt.table(
    name="silver_customer_profile",
    comment="Customer profile information with demographics",
    table_properties={"quality": "silver"}
)
def silver_customer_profile():
    # Synthetic customer profile data
    data = [
        {"customer_id": 1001, "signup_date": "2022-03-15", "age_group": "25-34", "gender": "F", "income_bracket": "50k-75k", "loyalty_tier": "Gold"},
        {"customer_id": 1002, "signup_date": "2021-11-20", "age_group": "35-44", "gender": "M", "income_bracket": "75k-100k", "loyalty_tier": "Silver"},
        {"customer_id": 1003, "signup_date": "2023-01-05", "age_group": "18-24", "gender": "F", "income_bracket": "25k-50k", "loyalty_tier": "Bronze"},
        {"customer_id": 1004, "signup_date": "2022-07-30", "age_group": "45-54", "gender": "M", "income_bracket": "100k+", "loyalty_tier": "Gold"},
        {"customer_id": 1005, "signup_date": "2023-05-12", "age_group": "25-34", "gender": "M", "income_bracket": "50k-75k", "loyalty_tier": "Bronze"}
    ]
    
    return spark.createDataFrame(data)

# FEATURE LAYER
# Create features for machine learning models
@dlt.table(
    name="features_customer_engagement",
    comment="Customer engagement features for ML models",
    table_properties={"quality": "gold"}
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
            max("activity_date").alias("last_activity_date"),
            countDistinct("activity_date").alias("active_days"),
            approx_count_distinct(
                when(col("is_weekend") == True, col("activity_date"))
            ).alias("weekend_activity_days")
        )
    )

@dlt.table(
    name="features_customer_behavior",
    comment="Customer behavior patterns for ML models",
    table_properties={"quality": "gold"}
)
def features_customer_behavior():
    return (
        dlt.read("silver_customer_activity")
        .groupBy("customer_id", "device")
        .agg(count("*").alias("session_count"))
        .groupBy("customer_id")
        .pivot("device")
        .sum("session_count")
        .fillna(0)
        .withColumnRenamed("desktop", "desktop_sessions")
        .withColumnRenamed("mobile", "mobile_sessions")
        .withColumnRenamed("tablet", "tablet_sessions")
    )

@dlt.table(
    name="ml_feature_table",
    comment="Combined feature table ready for ML training",
    table_properties={"quality": "gold"}
)
def ml_feature_table():
    engagement_features = dlt.read("features_customer_engagement")
    behavior_features = dlt.read("features_customer_behavior")
    customer_profile = dlt.read("silver_customer_profile")
    
    # Join all feature tables
    return (
        engagement_features
        .join(behavior_features, "customer_id", "inner")
        .join(customer_profile, "customer_id", "inner")
        .withColumn("feature_timestamp", current_timestamp())
        .withColumn("days_since_signup", 
                    datediff(current_date(), to_date(col("signup_date"))))
        .withColumn("churn_risk_score", 
                    when(col("avg_engagement_score") > 15, "low")
                    .when(col("avg_engagement_score") > 5, "medium")
                    .otherwise("high"))
    ) 