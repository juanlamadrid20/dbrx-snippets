# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Table (DLT) - Medallion Architecture Example
# MAGIC 
# MAGIC This notebook demonstrates how to work with a Delta Live Table pipeline implementing the medallion architecture:
# MAGIC 
# MAGIC - **Bronze Layer**: Raw data ingestion from source
# MAGIC - **Silver Layer**: Cleaned, validated, and transformed data
# MAGIC - **Gold Layer**: Business-level aggregates and metrics
# MAGIC 
# MAGIC ## What is the Medallion Architecture?
# MAGIC 
# MAGIC The medallion architecture is a data design pattern that organizes data into different layers:
# MAGIC 
# MAGIC - **Bronze** (Raw): Data is stored in its original form
# MAGIC - **Silver** (Refined): Data is cleansed, validated, and transformed
# MAGIC - **Gold** (Aggregated): Business-level aggregates and metrics
# MAGIC 
# MAGIC ![Medallion Architecture](https://github.com/databricks-demos/dbdemos-resources/raw/main/images/db-228-lakehouse-explained.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sample Data
# MAGIC 
# MAGIC First, let's generate sample e-commerce data for our pipeline.

# COMMAND ----------

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

# COMMAND ----------

# Generate sample data
orders_df = generate_orders(1000)

# Display sample data
display(orders_df.limit(10))

# COMMAND ----------

# Save the sample data to a location that can be used as source for our DLT pipeline
# Note: In a real-world scenario, this would come from a source system
orders_df.write.mode("overwrite").format("json").save("/Volumes/juan_dev/data_eng/data/orders_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting Up a Delta Live Table Pipeline
# MAGIC 
# MAGIC To set up a DLT pipeline:
# MAGIC 
# MAGIC 1. Go to the Databricks Workflows page
# MAGIC 2. Click "Delta Live Tables"
# MAGIC 3. Click "Create Pipeline"
# MAGIC 4. Fill in the following information:
# MAGIC    - **Pipeline Name**: E-commerce Medallion Pipeline
# MAGIC    - **Product Edition**: Choose your edition
# MAGIC    - **Source Code**: Browse and select the `sample_dlt.py` file
# MAGIC    - **Storage Location**: Choose a location for DLT tables
# MAGIC    - **Target Database**: Choose a database or create a new one
# MAGIC 5. Click "Create"
# MAGIC 
# MAGIC ![Create DLT Pipeline](https://docs.databricks.com/_images/create-pipeline.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding the DLT Pipeline
# MAGIC 
# MAGIC The `sample_dlt.py` file defines a complete medallion architecture using DLT decorators:
# MAGIC 
# MAGIC ### Bronze Layer
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     name="bronze_orders",
# MAGIC     comment="Raw e-commerce orders ingested from JSON files",
# MAGIC     table_properties={
# MAGIC         "quality": "bronze",
# MAGIC         "pipelines.autoOptimize.zOrderCols": "order_id"
# MAGIC     }
# MAGIC )
# MAGIC def bronze_orders():
# MAGIC     return (
# MAGIC         spark.readStream.format("json")
# MAGIC         .load("/Volumes/juan_dev/data_eng/data/orders_raw")
# MAGIC     )
# MAGIC ```
# MAGIC 
# MAGIC ### Silver Layer
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     name="silver_orders",
# MAGIC     comment="Cleaned and validated orders",
# MAGIC     table_properties={
# MAGIC         "quality": "silver",
# MAGIC         "pipelines.autoOptimize.zOrderCols": "order_id,customer_id"
# MAGIC     }
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_order_date", "order_date IS NOT NULL AND length(order_date) = 10")
# MAGIC @dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_quantity", "quantity > 0")
# MAGIC def silver_orders():
# MAGIC     return (
# MAGIC         dlt.read("bronze_orders")
# MAGIC         .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
# MAGIC         .withColumn("status", coalesce(col("status"), lit("unknown")))
# MAGIC         .withColumn("total_item_price", col("price") * col("quantity"))
# MAGIC         .withColumn("processed_timestamp", current_timestamp())
# MAGIC     )
# MAGIC ```
# MAGIC 
# MAGIC ### Gold Layer
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     name="gold_daily_sales",
# MAGIC     comment="Daily sales aggregates by category",
# MAGIC     table_properties={"quality": "gold"}
# MAGIC )
# MAGIC def gold_daily_sales():
# MAGIC     return (
# MAGIC         dlt.read("silver_orders")
# MAGIC         .groupBy("order_date", "category")
# MAGIC         .agg(
# MAGIC             countDistinct("order_id").alias("total_orders"),
# MAGIC             sum("quantity").alias("total_items_sold"),
# MAGIC             sum("total_item_price").alias("total_revenue")
# MAGIC         )
# MAGIC         .orderBy("order_date", "category")
# MAGIC     )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Benefits of DLT
# MAGIC 
# MAGIC 1. **Declarative Data Transformations**: Focus on what, not how
# MAGIC 2. **Built-in Data Quality**: Expectations for data validation
# MAGIC 3. **Automatic Incremental Processing**: Efficiently process only new data
# MAGIC 4. **Data Lineage**: Track data through transformations
# MAGIC 5. **Production-ready Pipelines**: Monitoring, error handling, and recovery built-in

# COMMAND ----------

# MAGIC %md
# MAGIC ## After Running the Pipeline
# MAGIC 
# MAGIC Once the pipeline is created and run, you can query the tables:
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT * FROM your_database.bronze_orders LIMIT 10;
# MAGIC SELECT * FROM your_database.silver_orders LIMIT 10;
# MAGIC SELECT * FROM your_database.gold_daily_sales ORDER BY total_revenue DESC LIMIT 10;
# MAGIC ```
# MAGIC 
# MAGIC You can also see the data quality metrics in the DLT UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Data Expectations
# MAGIC 
# MAGIC DLT provides built-in data quality validation through expectations:
# MAGIC 
# MAGIC - `@dlt.expect`: Records data quality metrics but keeps all data
# MAGIC - `@dlt.expect_or_drop`: Drops records that don't meet expectations
# MAGIC - `@dlt.expect_or_fail`: Fails the pipeline if expectations aren't met
# MAGIC 
# MAGIC Our example uses `@dlt.expect_or_drop` to filter out bad data:
# MAGIC 
# MAGIC ```python
# MAGIC @dlt.expect_or_drop("valid_order_date", "order_date IS NOT NULL AND length(order_date) = 10")
# MAGIC @dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_quantity", "quantity > 0")
# MAGIC def silver_orders():
# MAGIC    ...
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Processing with DLT
# MAGIC 
# MAGIC DLT automatically handles incremental processing:
# MAGIC 
# MAGIC - For streaming tables, it tracks the latest data processed
# MAGIC - For triggered (batch) tables, it uses change data capture techniques
# MAGIC 
# MAGIC This means your pipeline only processes new data each run, making it efficient.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring and Maintenance
# MAGIC 
# MAGIC After your pipeline is running, you can:
# MAGIC 
# MAGIC 1. **Monitor pipeline health**: Metrics, data quality stats, and error logs
# MAGIC 2. **View data lineage**: Understand data flow and dependencies
# MAGIC 3. **Schedule pipeline runs**: Automate refresh cycles
# MAGIC 4. **Set up alerts**: Get notified of pipeline issues

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC 
# MAGIC In this notebook, we've:
# MAGIC 
# MAGIC 1. Generated sample e-commerce data
# MAGIC 2. Explored the medallion architecture implementation with DLT
# MAGIC 3. Learned how to set up a DLT pipeline
# MAGIC 4. Understood data expectations and quality checks
# MAGIC 5. Reviewed best practices for production pipelines
# MAGIC 
# MAGIC Delta Live Tables simplifies building reliable, maintainable data pipelines while enabling best practices like the medallion architecture. 