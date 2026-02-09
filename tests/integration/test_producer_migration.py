"""Integration test for producer migration"""
import pytest
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from src.producer.produce import produce_comment_events, teardown_topic
from src.producer.config import ProducerConfig


@pytest.fixture
def producer_config():
    """Create test producer configuration"""
    os.environ["KAFKA_BOOTSTRAP_LOCAL"] = "localhost:9092"
    os.environ["KAFKA_BOOTSTRAP_CONTAINERS"] = "broker:29092"
    os.environ["SCHEMA_REGISTRY_URL"] = "http://localhost:8081"
    os.environ["KAFKA_OUTPUT_TOPICS"] = "test-comments"
    
    return ProducerConfig()


def test_producer_config_loading(producer_config):
    """Test that producer config loads correctly"""
    assert producer_config.kafka_bootstrap_local == "localhost:9092"
    assert producer_config.output_topic == "test-comments"
    assert producer_config.batch_size == 1000


def test_get_bootstrap_servers(producer_config):
    """Test bootstrap server selection"""
    assert producer_config.get_bootstrap_servers("local") == "localhost:9092"
    assert producer_config.get_bootstrap_servers("containers") == "broker:29092"
    
    with pytest.raises(ValueError):
        producer_config.get_bootstrap_servers("invalid")


@patch("src.producer.produce.KafkaAdminClient")
@patch("src.producer.produce.KafkaProducerClient")
@patch("src.producer.produce.ParquetBatchReader")
def test_produce_comment_events_integration(
    mock_reader_class,
    mock_producer_class,
    mock_admin_class,
    producer_config,
    tmp_path
):
    """Test produce_comment_events with mocked dependencies"""
    # Mock admin client
    mock_admin = Mock()
    mock_admin_class.return_value = mock_admin
    
    # Mock producer
    mock_producer = Mock()
    mock_producer_class.return_value = mock_producer
    
    # Mock parquet reader
    import pandas as pd
    mock_reader = Mock()
    mock_df = pd.DataFrame({
        "reviewerID": ["user1", "user2"],
        "reviewText": ["Great!", "Nice!"]
    })
    mock_reader.iter_batches.return_value = [mock_df]
    mock_reader_class.return_value = mock_reader
    
    # Create dummy schema file
    schema_path = tmp_path / "avro_schemas" / "comment_events.avsc"
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text('{"type": "record", "name": "Comment"}')
    
    # Create dummy parquet file
    parquet_path = tmp_path / "data" / "user_comments.parquet"
    parquet_path.parent.mkdir(parents=True)
    mock_df.to_parquet(parquet_path)
    
    # Update config paths
    producer_config.avro_schema_path = str(schema_path)
    producer_config.review_parquet_path = str(parquet_path)
    
    # Run produce
    with patch("src.producer.produce.Path") as mock_path:
        mock_path.return_value.parent = tmp_path
        result = produce_comment_events(producer_config, "local")
    
    # Verify
    assert result is True
    mock_admin.create_topic.assert_called_once()
    assert mock_producer.produce.call_count == 2
    mock_producer.flush.assert_called_once()
