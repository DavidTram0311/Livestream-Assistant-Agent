"""Pytest configuration and fixtures"""
import pytest
import os
from pathlib import Path

# Set test environment
os.environ["TESTING"] = "1"
# Set required Kafka environment variables for tests
os.environ.setdefault("SCHEMA_REGISTRY_URL", "http://localhost:8081")

@pytest.fixture
def test_data_dir():
    """Return path to test data directory"""
    return Path(__file__).parent / "data"
    
@pytest.fixture
def sample_parquet_file(test_data_dir, tmp_path):
    """Create a sample parquet file for testing"""
    import pandas as pd
    import pyarrow.parquet as pq
    
    # Create sample data
    df = pd.DataFrame({
        "comment_id": [1, 2, 3, 4, 5],
        "user_id": ["user1", "user2", "user3", "user4", "user5"],
        "comments": ["Great!", "Nice product", "Amazing", "Love it", "Excellent"],
        "event_timestamp": [1000, 2000, 3000, 4000, 5000]
    })
    
    # Save to parquet
    parquet_file = tmp_path / "test_data.parquet"
    df.to_parquet(parquet_file, index=False)
    
    return str(parquet_file)