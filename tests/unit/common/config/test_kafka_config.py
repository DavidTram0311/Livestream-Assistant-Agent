"""Unit tests for Kafka configuration"""
import pytest
from pydantic import ValidationError
import os

def test_kafka_config_valid():
    """Test valid Kafka configuration"""
    from src.common.config.kafka_config import KafkaConfig
    
    os.environ["BOOTSTRAP_SERVERS"] = "localhost:9092"
    os.environ["SCHEMA_REGISTRY_URL"] = "http://localhost:8081"
    
    config = KafkaConfig()
    
    assert config.bootstrap_servers == "localhost:9092"
    assert config.schema_registry_url == "http://localhost:8081"
    assert config.num_partitions == 5
    assert config.replication_factor == 3

def test_kafka_config_custom_values():
    """Test Kafka configuration with custom values"""
    from src.common.config.kafka_config import KafkaConfig
    
    os.environ["BOOTSTRAP_SERVERS"] = "broker1:9092,broker2:9092"
    os.environ["SCHEMA_REGISTRY_URL"] = "https://schema-registry:8081"
    os.environ["NUM_PARTITIONS"] = "10"
    os.environ["REPLICATION_FACTOR"] = "2"
    
    config = KafkaConfig()
    
    assert config.num_partitions == 10
    assert config.replication_factor == 2
    
def test_kafka_config_validation_errors():
    """Test Kafka configuration validation"""
    from src.common.config.kafka_config import KafkaConfig
    
    # Missing required field
    os.environ.pop("BOOTSTRAP_SERVERS", None)
    with pytest.raises(ValidationError):
        KafkaConfig()
    
    # Invalid schema registry URL
    os.environ["BOOTSTRAP_SERVERS"] = "localhost:9092"
    os.environ["SCHEMA_REGISTRY_URL"] = "invalid-url"
    
    with pytest.raises(ValidationError) as exc_info:
        KafkaConfig()
    assert "schema_registry_url" in str(exc_info.value)