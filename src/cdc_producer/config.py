"""CDC Producer module configuration"""
from pydantic import Field
from common.config import PostgresConfig


class CDCProducerConfig(PostgresConfig):
    """CDC Producer-specific configuration"""
    
    # Data paths
    review_parquet_path: str = Field(
        default="data/user_comments.parquet",
        description="Path to review parquet file (relative to cdc_producer dir)",
        validation_alias="REVIEW_PARQUET_PATH"
    )
    
    # Batch processing
    batch_size: int = Field(
        default=10,
        description="Batch size for event production",
        validation_alias="BATCH_SIZE"
    )
    
    # Processing limits
    max_records: int = Field(
        default=100,
        description="Maximum records to process per run",
        validation_alias="MAX_RECORDS"
    )
    
    def get_absolute_parquet_path(self, base_dir: str) -> str:
        """Get absolute path to parquet file"""
        import os
        return os.path.join(base_dir, self.review_parquet_path)