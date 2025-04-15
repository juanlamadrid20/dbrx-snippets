# Delta Live Table Pipeline with Medallion Architecture
# This file demonstrates a medallion architecture implementation using Delta Live Tables

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Helper function to generate sample data (for testing in notebook only)
def generate_orders_data(spark, num_orders=1000):
    """Generate sample e-commerce order data"""
    from datetime import datetime, timedelta
    import random
    
    # List of possible products
    products = [
        {"product_id": 1, "name": "Laptop", "category": "Electronics", "price": 1200.0},
        {"product_id": 2, "name": "Smartphone", "category": "Electronics", "price": 800.0},
        {"product_id": 3, "name": "Headphones", "category": "Electronics", "price": 150.0},
        {"product_id": 4, "name": "T-shirt", "category": "Clothing", "price": 25.0},
        {"product_id": 5, "name": "Jeans", "category": "Clothing", "price": 60.0},
        {"product_id": 6, "name": "Sneakers", "category": "Footwear", "price": 120.0},
        {"product_id": 7, "name": "Coffee Maker", "category": "Home", "price": 89.0},
        {"product_id": 8, "name": "Blender", "category": "Home", "price": 49.0}
    ]
    
    # Generate orders
    orders = []
    for i in range(1, num_orders + 1):
        customer_id = random.randint(1, 100)
        order_date = (datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
        status = random.choice(["completed", "pending", "cancelled", None])  # Some orders have null status
        
        # Add error data occasionally
        if random.random() < 0.05:  # 5% of orders have data issues
            order_date = "invalid-date"  # Invalid date format
        
        # Select 1-3 products for this order
        num_products = random.randint(1, 3)
        order_products = random.sample(products, num_products)
        
        for product in order_products:
            quantity = random.randint(1, 5)
            
            # Sometimes introduce missing product_id for data quality issues
            product_id = product["product_id"] if random.random() > 0.03 else None
            
            orders.append({
                "order_id": i,
                "customer_id": customer_id,
                "order_date": order_date,
                "status": status,
                "product_id": product_id,
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"],
                "quantity": quantity
            })
    
    # Create DataFrame
    return spark.createDataFrame(orders)

# BRONZE LAYER
# The bronze layer ingests raw data with minimal or no transformations
@dlt.table(
    name="bronze_orders",
    comment="Raw e-commerce orders ingested from JSON files",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "order_id"
    }
)
def bronze_orders():
    return (
        spark.readStream.format("json")
        .load("/Volumes/juan_dev/data_eng/data/orders_raw")
    )

# SILVER LAYER
# The silver layer cleans and validates the data from the bronze layer
@dlt.table(
    name="silver_orders",
    comment="Cleaned and validated orders",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "order_id,customer_id"
    }
)
@dlt.expect_or_drop("valid_order_date", "order_date IS NOT NULL AND length(order_date) = 10")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
def silver_orders():
    return (
        dlt.read("bronze_orders")
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("status", coalesce(col("status"), lit("unknown")))
        .withColumn("total_item_price", col("price") * col("quantity"))
        .withColumn("processed_timestamp", current_timestamp())
    )

@dlt.table(
    name="silver_customers", 
    comment="Customer information derived from orders",
    table_properties={"quality": "silver"}
)
def silver_customers():
    return (
        dlt.read("silver_orders")
        .select("customer_id")
        .distinct()
        .withColumn("first_seen", current_date())
    )

# GOLD LAYER
# The gold layer contains business-level aggregates and metrics ready for consumption
@dlt.table(
    name="gold_daily_sales",
    comment="Daily sales aggregates by category",
    table_properties={"quality": "gold"}
)
def gold_daily_sales():
    return (
        dlt.read("silver_orders")
        .groupBy("order_date", "category")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("quantity").alias("total_items_sold"),
            sum("total_item_price").alias("total_revenue")
        )
        .orderBy("order_date", "category")
    )

@dlt.table(
    name="gold_customer_insights",
    comment="Customer purchase insights",
    table_properties={"quality": "gold"}
)
def gold_customer_insights():
    return (
        dlt.read("silver_orders")
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("total_item_price").alias("lifetime_value"),
            avg("total_item_price").alias("avg_order_value"),
            max("order_date").alias("latest_order_date")
        )
    )

@dlt.table(
    name="gold_product_performance",
    comment="Product sales performance metrics",
    table_properties={"quality": "gold"}
)
def gold_product_performance():
    return (
        dlt.read("silver_orders")
        .groupBy("product_id", "product_name", "category")
        .agg(
            sum("quantity").alias("total_units_sold"),
            sum("total_item_price").alias("total_revenue"),
            avg("price").alias("avg_price")
        )
        .orderBy(col("total_revenue").desc())
    ) 