# Databricks notebook source
# MAGIC %md
# MAGIC # Data Generation Job
# MAGIC This notebook generates sample e-commerce order data for testing and development purposes.
# MAGIC
# MAGIC The generated data includes:
# MAGIC - Orders with customer IDs
# MAGIC - Product details and categories 
# MAGIC - Order dates, quantities and status
# MAGIC - Some intentional data quality issues for testing

import random
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Helper function to generate sample data
def generate_orders(num_orders=1000):
    """Generate sample e-commerce order data"""
    
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


# Generate sample data
orders_df = generate_orders(2000)

# Display sample data
display(orders_df.limit(10))


# Save the sample data to a location that can be used as source for our DLT pipeline
# Note: In a real-world scenario, this would come from a source system
orders_df.write.mode("overwrite").format("json").save("/Volumes/juan_dev/data_eng/data/orders_raw")