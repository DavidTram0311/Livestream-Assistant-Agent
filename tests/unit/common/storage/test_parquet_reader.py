"""Unit tests for ParquetBatchReader"""
import pytest
import pandas as pd

def test_parquet_reader_initialization(sample_parquet_file):
    """Test ParquetBatchReader initialization"""
    from src.common.storage.parquet_reader import ParquetBatchReader
    
    reader = ParquetBatchReader(sample_parquet_file, batch_size=2)
    
    assert reader.batch_size == 2
    assert reader.parquet_file is not None

def test_parquet_reader_file_not_found():
    """Test ParquetBatchReader with non-existent file"""
    from src.common.storage.parquet_reader import ParquetBatchReader
    
    with pytest.raises(FileNotFoundError):
        ParquetBatchReader("non_existent_file.parquet")

def test_parquet_reader_iter_batches(sample_parquet_file):
    """Test iterating over batches"""
    from src.common.storage.parquet_reader import ParquetBatchReader
    
    reader = ParquetBatchReader(sample_parquet_file, batch_size=2)
    
    batches = list(reader.iter_batches())
    
    # Should have 3 batches (5 records with batch_size=2)
    assert len(batches) == 3
    assert len(batches[0]) == 2
    assert len(batches[1]) == 2
    assert len(batches[2]) == 1

def test_parquet_reader_max_records(sample_parquet_file):
    """Test max_records parameter"""
    from src.common.storage.parquet_reader import ParquetBatchReader
    
    reader = ParquetBatchReader(sample_parquet_file, batch_size=2)
    
    batches = list(reader.iter_batches(max_records=3))
    
    # Should stop after 3 records (2 batches)
    assert len(batches) == 2
    total_records = sum(len(batch) for batch in batches)
    assert total_records == 4  # 2 + 2 (stops after reaching 3+)

def test_parquet_reader_transform(sample_parquet_file):
    """Test batch transformation"""
    from src.common.storage.parquet_reader import ParquetBatchReader
    
    def uppercase_comments(df):
        df["comments"] = df["comments"].str.upper()
        return df
    
    reader = ParquetBatchReader(sample_parquet_file, batch_size=2)
    
    batches = list(reader.iter_batches(transform=uppercase_comments))
    
    # Check that transformation was applied
    for batch in batches:
        assert all(batch["comments"].str.isupper())

def test_parquet_reader_metadata(sample_parquet_file):
    """Test getting parquet metadata"""
    from src.common.storage.parquet_reader import ParquetBatchReader
    
    reader = ParquetBatchReader(sample_parquet_file)
    metadata = reader.get_metadata()
    
    assert metadata["num_rows"] == 5
    assert metadata["num_columns"] == 4
    assert "num_row_groups" in metadata