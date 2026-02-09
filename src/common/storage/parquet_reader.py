"""Parquet batch reader utility"""
import pyarrow.parquet as pq
from typing import Iterator, Callable, Optional, Dict, Any
import pandas as pd
from pathlib import Path
from src.common.logging import get_logger

logger = get_logger(__name__)

class ParquetBatchReader:
    """
    Batch reader for parquet files with progress tracking and transformation support.
    
    Example:
        reader = ParquetBatchReader("data/comments.parquet", batch_size=1000)
        for batch_df in reader.iter_batches(max_records=10000):
            # Process batch_df
            pass
    """
    
    def __init__(self, file_path: str, batch_size: int = 1000):
        """
        Initialize parquet reader.
        
        Args:
            file_path: Path to parquet file
            batch_size: Number of records per batch
        
        Raises:
            FileNotFoundError: If parquet file doesn't exist
            ValueError: If batch_size < 1
        """
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {file_path}")
        
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        
        self.batch_size = batch_size
        self.parquet_file = pq.ParquetFile(str(file_path))
        
        logger.info(f"Initialized ParquetBatchReader for {file_path} with batch_size={batch_size}")
    
    def iter_batches(
        self,
        transform: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        max_records: Optional[int] = None,
        columns: Optional[list[str]] = None,
        filters: Optional[list] = None,
    ) -> Iterator[pd.DataFrame]:
        """
        Iterate over batches with optional transformation and filtering.
        
        Args:
            transform: Optional function to transform each batch DataFrame
            max_records: Maximum total records to process (None = all)
            columns: List of columns to read (None = all)
            filters: PyArrow filters to apply
        
        Yields:
            DataFrame batches
        """
        total_processed = 0
        batch_num = 0
        
        for batch in self.parquet_file.iter_batches(
            batch_size=self.batch_size,
            columns=columns,
        ):
            batch_df = batch.to_pandas()
            batch_num += 1
            
            # Apply transformation if provided
            if transform:
                try:
                    batch_df = transform(batch_df)
                except Exception as e:
                    logger.error(f"Error transforming batch {batch_num}: {e}")
                    raise
            
            yield batch_df
            
            total_processed += len(batch_df)
            logger.debug(f"Processed batch {batch_num}: {len(batch_df)} records (total: {total_processed})")
            
            # Check max_records limit
            if max_records and total_processed >= max_records:
                logger.info(f"Reached max_records limit: {max_records}")
                break
        
        logger.info(f"Completed processing {total_processed} records in {batch_num} batches")
    
    def get_schema(self):
        """Get parquet file schema"""
        return self.parquet_file.schema
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get parquet file metadata"""
        metadata = self.parquet_file.metadata
        return {
            "num_rows": metadata.num_rows,
            "num_columns": metadata.num_columns,
            "num_row_groups": metadata.num_row_groups,
            "serialized_size": metadata.serialized_size,
        }
    
    def __repr__(self) -> str:
        return f"ParquetBatchReader(file_path='{self.file_path}', batch_size={self.batch_size})"