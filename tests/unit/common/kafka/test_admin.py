"""Unit tests for Kafka admin client"""
import pytest
from unittest.mock import Mock, patch

def test_kafka_admin_initialization():
    """Test KafkaAdminClient initialization"""
    from src.common.kafka.admin import KafkaAdminClient
    
    with patch("src.common.kafka.admin.AdminClient") as mock_admin:
        admin = KafkaAdminClient("localhost:9092")
        
        mock_admin.assert_called_once_with({"bootstrap.servers": "localhost:9092"})
        assert admin.bootstrap_servers == "localhost:9092"

def test_kafka_admin_create_topic():
    """Test topic creation"""
    from src.common.kafka.admin import KafkaAdminClient
    
    with patch("src.common.kafka.admin.AdminClient") as mock_admin_class:
        mock_admin = Mock()
        mock_admin_class.return_value = mock_admin
        
        # Mock successful topic creation
        mock_future = Mock()
        mock_future.result.return_value = None
        mock_admin.create_topics.return_value = {"test-topic": mock_future}
        
        admin = KafkaAdminClient("localhost:9092")
        result = admin.create_topic("test-topic", num_partitions=5, replication_factor=3)
        
        assert result is True
        mock_admin.create_topics.assert_called_once()

def test_kafka_admin_delete_topic():
    """Test topic deletion"""
    from src.common.kafka.admin import KafkaAdminClient
    
    with patch("src.common.kafka.admin.AdminClient") as mock_admin_class:
        mock_admin = Mock()
        mock_admin_class.return_value = mock_admin
        
        # Mock successful topic deletion
        mock_future = Mock()
        mock_future.result.return_value = None
        mock_admin.delete_topics.return_value = {"test-topic": mock_future}
        
        admin = KafkaAdminClient("localhost:9092")
        result = admin.delete_topic("test-topic")
        
        assert result is True
        mock_admin.delete_topics.assert_called_once_with(["test-topic"])