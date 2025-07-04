# Delta Live Tables Pipeline Configuration
# Centralized configuration for the enhanced medallion architecture pipeline

from dataclasses import dataclass
from typing import Dict, List, Optional
import os

@dataclass
class PipelineConfig:
    """Configuration class for Delta Live Tables pipeline"""
    
    # Environment Configuration
    catalog: str = "juan_dev"
    schema: str = "data_eng"
    environment: str = "dev"  # dev, staging, prod
    
    # Source Configuration
    source_base_path: str = f"/Volumes/{catalog}/{schema}/data"
    orders_source_path: str = f"{source_base_path}/orders_raw"
    
    # Performance Configuration
    target_file_size_mb: int = 128
    max_files_per_trigger: int = 1000
    max_bytes_per_trigger: str = "1gb"
    watermark_delay: str = "10 minutes"
    
    # Retention Configuration
    log_retention_days: int = 60
    deleted_file_retention_days: int = 30
    
    # Data Quality Configuration
    max_quantity: int = 10000
    max_price: float = 100000.0
    
    # Business Logic Configuration
    premium_customer_threshold: float = 1000.0
    standard_customer_threshold: float = 500.0
    low_price_threshold: float = 10.0
    medium_price_threshold: float = 100.0
    
    # Customer Segmentation Thresholds
    frequent_customer_orders: int = 10
    regular_customer_orders: int = 5
    occasional_customer_orders: int = 2
    
    # Recency Segmentation (days)
    recent_days_threshold: int = 30
    moderately_recent_days_threshold: int = 90
    at_risk_days_threshold: int = 180
    
    @property
    def target_file_size_bytes(self) -> int:
        """Convert target file size from MB to bytes"""
        return self.target_file_size_mb * 1024 * 1024
    
    @property
    def log_retention_interval(self) -> str:
        """Format log retention as interval string"""
        return f"interval {self.log_retention_days} days"
    
    @property
    def deleted_file_retention_interval(self) -> str:
        """Format deleted file retention as interval string"""
        return f"interval {self.deleted_file_retention_days} days"
    
    def get_table_name(self, layer: str, entity: str) -> str:
        """Generate fully qualified table name"""
        return f"{self.catalog}.{self.schema}.{layer}_{entity}"
    
    def get_base_table_properties(self) -> Dict[str, str]:
        """Get common table properties for all tables"""
        return {
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
            "delta.enableChangeDataFeed": "true",
            "delta.enableDeletionVectors": "true",
            "delta.targetFileSize": str(self.target_file_size_bytes),
            "delta.logRetentionDuration": self.log_retention_interval,
            "delta.deletedFileRetentionDuration": self.deleted_file_retention_interval,
            "delta.isolationLevel": "WriteSerializable"
        }
    
    def get_bronze_table_properties(self, z_order_cols: List[str]) -> Dict[str, str]:
        """Get table properties specific to bronze layer"""
        props = self.get_base_table_properties()
        props.update({
            "quality": "bronze",
            "pipelines.autoOptimize.zOrderCols": ",".join(z_order_cols)
        })
        return props
    
    def get_silver_table_properties(self, z_order_cols: List[str]) -> Dict[str, str]:
        """Get table properties specific to silver layer"""
        props = self.get_base_table_properties()
        props.update({
            "quality": "silver",
            "pipelines.autoOptimize.zOrderCols": ",".join(z_order_cols)
        })
        return props
    
    def get_gold_table_properties(self, z_order_cols: List[str]) -> Dict[str, str]:
        """Get table properties specific to gold layer"""
        props = self.get_base_table_properties()
        props.update({
            "quality": "gold",
            "pipelines.autoOptimize.zOrderCols": ",".join(z_order_cols)
        })
        return props
    
    def get_streaming_options(self) -> Dict[str, str]:
        """Get streaming configuration options"""
        return {
            "cloudFiles.format": "json",
            "cloudFiles.schemaEvolutionMode": "addNewColumns",
            "cloudFiles.inferColumnTypes": "false",
            "maxFilesPerTrigger": str(self.max_files_per_trigger),
            "cloudFiles.maxBytesPerTrigger": self.max_bytes_per_trigger
        }

# Environment-specific configurations
ENVIRONMENT_CONFIGS = {
    "dev": PipelineConfig(
        catalog="juan_dev",
        schema="data_eng",
        environment="dev",
        max_files_per_trigger=100,  # Smaller batches for dev
        target_file_size_mb=64,     # Smaller files for dev
    ),
    "staging": PipelineConfig(
        catalog="staging",
        schema="data_eng",
        environment="staging",
        max_files_per_trigger=500,
        target_file_size_mb=128,
    ),
    "prod": PipelineConfig(
        catalog="prod",
        schema="data_eng", 
        environment="prod",
        max_files_per_trigger=1000,
        target_file_size_mb=256,    # Larger files for production
        log_retention_days=90,      # Longer retention in prod
        deleted_file_retention_days=60,
    )
}

def get_config(environment: Optional[str] = None) -> PipelineConfig:
    """
    Get configuration for specified environment
    
    Args:
        environment: Environment name (dev, staging, prod)
                    If None, will try to detect from environment variables
    
    Returns:
        PipelineConfig instance for the specified environment
    """
    if environment is None:
        environment = os.getenv("PIPELINE_ENV", "dev")
    
    if environment not in ENVIRONMENT_CONFIGS:
        raise ValueError(f"Unknown environment: {environment}. Valid options: {list(ENVIRONMENT_CONFIGS.keys())}")
    
    return ENVIRONMENT_CONFIGS[environment]

# Data Quality Expectations Templates
DATA_QUALITY_EXPECTATIONS = {
    "bronze": {
        "not_null_checks": [
            ("valid_order_id", "order_id IS NOT NULL AND length(trim(order_id)) > 0"),
            ("valid_customer_id", "customer_id IS NOT NULL AND length(trim(customer_id)) > 0"),
            ("valid_product_id", "product_id IS NOT NULL AND length(trim(product_id)) > 0"),
        ],
        "format_checks": [
            ("valid_order_date", "order_date IS NOT NULL AND length(order_date) = 10"),
            ("valid_order_date_format", "order_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'"),
        ]
    },
    "silver": {
        "business_rules": [
            ("valid_quantity", lambda config: f"quantity > 0 AND quantity <= {config.max_quantity}"),
            ("valid_price", lambda config: f"price > 0 AND price <= {config.max_price}"),
        ],
        "critical_checks": [
            ("critical_data_quality", "count(*) > 0"),
        ]
    }
}

# Schema Definitions
BRONZE_SCHEMA_COLUMNS = [
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

# Export the default configuration
config = get_config() 