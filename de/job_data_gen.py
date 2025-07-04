# Databricks notebook source
# MAGIC %md
# MAGIC # Data Generation Job
# MAGIC This notebook generates comprehensive synthetic e-commerce data for testing and development purposes.
# MAGIC
# MAGIC The generated data includes:
# MAGIC - Orders with customer IDs, products, and pricing
# MAGIC - Customer activity logs (page views, cart adds, purchases)
# MAGIC - Customer profile data (demographics, signup dates)
# MAGIC - Some intentional data quality issues for testing DLT expectations

# COMMAND ----------
import random
import uuid
from datetime import datetime, timedelta
from pyspark.sql import functions as F
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


def generate_customer_profiles(num_customers=100):
    """Generate customer profile data for ML features"""
    
    age_groups = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
    genders = ["M", "F", "Other"]
    income_brackets = ["25k-50k", "50k-75k", "75k-100k", "100k+"]
    loyalty_tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    
    profiles = []
    for i in range(1, num_customers + 1):
        # Generate signup date between 6 months and 3 years ago
        days_ago = random.randint(180, 1095)
        signup_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        
        profiles.append({
            "customer_id": i,
            "signup_date": signup_date,
            "age_group": random.choice(age_groups),
            "gender": random.choice(genders),
            "income_bracket": random.choice(income_brackets),
            "loyalty_tier": random.choice(loyalty_tiers)
        })
    
    return spark.createDataFrame(profiles)


def generate_customer_activity(num_customers=100, days_back=30, sessions_per_day=5):
    """Generate realistic customer activity data for ML features"""
    
    devices = ["desktop", "mobile", "tablet"]
    locations = ["New York", "San Francisco", "Chicago", "Austin", "Seattle", "Boston", "Denver", "Miami"]
    
    activities = []
    
    for customer_id in range(1, num_customers + 1):
        # Generate activity for the last N days
        for day_offset in range(days_back):
            activity_date = datetime.now() - timedelta(days=day_offset)
            
            # Not all customers are active every day (70% chance of activity)
            if random.random() > 0.7:
                continue
                
            # Generate 1-10 sessions per active day
            num_sessions = random.randint(1, sessions_per_day)
            
            for _ in range(num_sessions):
                # Generate realistic session timestamp
                hour = random.randint(8, 23)  # Active hours 8am-11pm
                minute = random.randint(0, 59)
                timestamp = activity_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
                
                # Generate realistic activity patterns
                device = random.choice(devices)
                location = random.choice(locations)
                
                # Page views follow a realistic distribution
                page_views = max(1, int(random.expovariate(0.2)))  # Exponential distribution
                page_views = min(page_views, 50)  # Cap at 50 page views
                
                # Cart adds are less frequent than page views
                cart_adds = 0
                if random.random() < 0.3:  # 30% chance of cart add
                    cart_adds = random.randint(1, min(5, page_views // 2))
                
                # Purchases are even less frequent
                purchases = 0
                if cart_adds > 0 and random.random() < 0.4:  # 40% conversion from cart to purchase
                    purchases = random.randint(1, cart_adds)
                
                # Add some data quality issues (5% of records)
                if random.random() < 0.05:
                    # Introduce various data quality issues
                    issue_type = random.choice(["null_customer", "invalid_timestamp", "negative_values"])
                    if issue_type == "null_customer":
                        customer_id = None
                    elif issue_type == "invalid_timestamp":
                        timestamp = "invalid-timestamp"
                    elif issue_type == "negative_values":
                        page_views = -1
                
                activities.append({
                    "customer_id": customer_id,
                    "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else timestamp,
                    "page_views": page_views,
                    "cart_adds": cart_adds,
                    "purchases": purchases,
                    "device": device,
                    "location": location,
                    "session_id": str(uuid.uuid4())  # Unique session identifier
                })
    
    return spark.createDataFrame(activities)

# COMMAND ----------
# Generate all datasets
print("Generating e-commerce order data...")
orders_df = generate_orders(2000)

print("Generating customer profile data...")
profiles_df = generate_customer_profiles(100)

print("Generating customer activity data...")
activity_df = generate_customer_activity(num_customers=100, days_back=30, sessions_per_day=8)

# Display sample data
print("Sample Orders Data:")
display(orders_df.limit(10))

print("Sample Customer Profiles:")
display(profiles_df.limit(10))

print("Sample Customer Activity:")
display(activity_df.limit(10))

# Data quality summary
print(f"Generated {orders_df.count()} order records")
print(f"Generated {profiles_df.count()} customer profiles")
print(f"Generated {activity_df.count()} activity records")

# Save the datasets to locations that can be used as sources for our DLT pipelines
print("Saving datasets...")

# Save orders data (for main pipeline)
orders_df.write.mode("overwrite").format("json").save("/Volumes/juan_dev/data_eng/data/orders_raw")
print("✓ Orders data saved to: /Volumes/juan_dev/data_eng/data/orders_raw")

# Save customer activity data (for features pipeline)
activity_df.write.mode("overwrite").format("json").save("/Volumes/juan_dev/data_eng/data/customer_activity_raw")
print("✓ Customer activity data saved to: /Volumes/juan_dev/data_eng/data/customer_activity_raw")

# Save customer profiles (for features pipeline)
profiles_df.write.mode("overwrite").format("json").save("/Volumes/juan_dev/data_eng/data/customer_profiles_raw")
print("✓ Customer profiles saved to: /Volumes/juan_dev/data_eng/data/customer_profiles_raw")

print("Data generation completed successfully!")