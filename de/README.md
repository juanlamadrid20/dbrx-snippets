# Delta Live Tables - Medallion Architecture Example

This directory contains examples for building data pipelines using Delta Live Tables (DLT) with the medallion architecture.

## Files in this directory

- `sample_dlt.py`: The DLT pipeline implementation with bronze, silver, and gold tables
- `sample_dlt_notebook.py`: A tutorial notebook that explains DLT concepts and demonstrates usage

## What is the Medallion Architecture?

The medallion architecture is a data design pattern that organizes data into different layers:

- **Bronze** (Raw): Data is stored in its original form
- **Silver** (Refined): Data is cleansed, validated, and transformed
- **Gold** (Aggregated): Business-level aggregates and metrics

## Getting Started

1. **Set up your Databricks workspace**:
   - Ensure you have a Databricks workspace with the necessary permissions to create DLT pipelines
   - This example uses a Databricks Volume at `/Volumes/juan_dev/data_eng/data` for data storage

2. **Run the sample notebook**:
   - Upload `sample_dlt_notebook.py` to your Databricks workspace
   - Run the notebook to generate sample data and learn about DLT concepts

3. **Create a DLT pipeline**:
   - Upload `sample_dlt.py` to your Databricks workspace
   - Go to the Workflows section and select "Delta Live Tables"
   - Create a new pipeline using `sample_dlt.py` as the source
   - Configure pipeline settings (database, storage location, etc.)
   - Start the pipeline

## Sample Dataset

The example uses a simulated e-commerce dataset with:
- Customer orders
- Product information
- Order details (date, status, etc.)

The data generation includes intentional quality issues to demonstrate DLT's data quality features.

## Key Features Demonstrated

- **DLT decorators**: `@dlt.table`, `@dlt.expect_or_drop`
- **Data quality validation**: Expectations to validate and filter data
- **Table properties**: Configuration for optimization and metadata
- **Medallion architecture**: Structured data transformation in layers

## Learn More

- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [Medallion Architecture Guide](https://www.databricks.com/glossary/medallion-architecture)
- [DLT Python API Reference](https://docs.databricks.com/delta-live-tables/python-ref.html)
- [Databricks Volumes](https://docs.databricks.com/en/storage/volumes.html) 