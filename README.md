# Delta Live Tables Demo with Databricks Asset Bundles

This project demonstrates how to implement a medallion architecture data pipeline using Delta Live Tables and how to deploy it using Databricks Asset Bundles.

## Project Structure

- `de/sample_dlt.py`: The DLT pipeline implementation with bronze, silver, and gold tables
- `de/sample_dlt_notebook.py`: A tutorial notebook that explains DLT concepts and demonstrates usage
- `de/README.md`: Documentation about the medallion architecture and DLT features
- `databricks.yml`: Databricks Asset Bundle configuration file

## Data Storage

This project uses a Databricks Volume for data storage at: `/Volumes/juan_dev/data_eng/data`

## Deploying with Databricks Asset Bundles

1. **Prerequisites**:
   - Databricks CLI v0.218.0 or later installed
   - Authentication set up for your Databricks workspace

2. **Validate the bundle**:
   ```
   databricks bundle validate
   ```

3. **Deploy the bundle to the development environment**:
   ```
   databricks bundle deploy
   ```
   or
   ```
   databricks bundle deploy -t dev
   ```

4. **Generate sample data**:
   ```
   databricks bundle run -t dev generate_sample_data
   ```

5. **Deploy to production** (when ready):
   ```
   databricks bundle deploy -t prod
   ```

## Working with the Pipeline

After deployment, you can:

1. View the deployed DLT pipeline in the Databricks workspace
2. Monitor the pipeline execution and data quality
3. Query the created tables in their respective layers (bronze, silver, gold)

## Medallion Architecture

This project demonstrates the medallion architecture with:

- **Bronze Layer**: Raw data ingestion from source
- **Silver Layer**: Cleaned, validated, and transformed data
- **Gold Layer**: Business-level aggregates and metrics

## Learn More

- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Medallion Architecture Guide](https://www.databricks.com/glossary/medallion-architecture)