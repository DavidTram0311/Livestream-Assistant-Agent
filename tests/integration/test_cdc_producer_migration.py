"""Integration test for CDC producer migration"""
import pytest
import os
from unittest.mock import Mock, patch, MagicMock
from src.cdc_producer.cdc_produce import produce_event, format_event
from src.cdc_producer.config import CDCProducerConfig


@pytest.fixture
def cdc_config():
    """Create test CDC producer configuration"""
    os.environ["POSTGRES_HOST"] = "localhost"
    os.environ["POSTGRES_PORT"] = "5432"
    os.environ["POSTGRES_DB"] = "test_db"
    os.environ["POSTGRES_USER"] = "test_user"
    os.environ["POSTGRES_PASSWORD"] = "test_pass"
    
    return CDCProducerConfig()


def test_cdc_config_loading(cdc_config):
    """Test CDC config loads correctly"""
    assert cdc_config.host == "localhost"
    assert cdc_config.port == 5432
    assert cdc_config.database == "test_db"
    assert cdc_config.batch_size == 10
    assert cdc_config.max_records == 100


def test_format_event():
    """Test event formatting"""
    row = {"user_id": "user123", "comments": "Great product!"}
    event = format_event(row)
    
    assert event.user_id == "user123"
    assert event.comments == "Great product!"


@patch("src.cdc_producer.cdc_produce.PostgresClient")
@patch("src.cdc_producer.cdc_produce.ParquetBatchReader")
def test_produce_event_integration(
    mock_reader_class,
    mock_pg_client_class,
    cdc_config,
    tmp_path
):
    """Test produce_event with mocked dependencies"""
    # Mock PostgreSQL client
    mock_pg_client = MagicMock()
    mock_session = MagicMock()
    mock_pg_client.get_session.return_value.__enter__.return_value = mock_session
    mock_pg_client_class.return_value = mock_pg_client
    
    # Mock parquet reader
    import pandas as pd
    mock_reader = Mock()
    mock_df = pd.DataFrame({
        "reviewerID": ["user1", "user2"],
        "reviewText": ["Great!", "Nice!"]
    })
    mock_reader.iter_batches.return_value = [mock_df]
    mock_reader_class.return_value = mock_reader
    
    # Create dummy parquet file
    parquet_path = tmp_path / "data" / "user_comments.parquet"
    parquet_path.parent.mkdir(parents=True)
    mock_df.to_parquet(parquet_path)
    
    # Update config
    cdc_config.review_parquet_path = str(parquet_path)
    cdc_config.max_records = 10
    
    # Run produce
    with patch("src.cdc_producer.cdc_produce.Path") as mock_path:
        mock_path.return_value.parent = tmp_path
        produce_event(cdc_config)
    
    # Verify
    mock_session.bulk_save_objects.assert_called()
    mock_session.commit.assert_called()
    mock_pg_client.close.assert_called_once()