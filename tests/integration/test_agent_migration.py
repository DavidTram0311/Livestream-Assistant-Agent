"""Integration test for agent migration"""
import pytest
import os
from unittest.mock import Mock, patch, AsyncMock
from fastapi.testclient import TestClient


@pytest.fixture
def agent_config():
    """Create test agent configuration"""
    os.environ["REDIS_HOST"] = "localhost"
    os.environ["REDIS_PORT"] = "6379"
    os.environ["IS_APPLE_SILICON"] = "false"
    
    from src.agent.config import AgentConfig
    return AgentConfig()


def test_agent_config_loading(agent_config):
    """Test agent config loads correctly"""
    assert agent_config.host == "localhost"
    assert agent_config.port == 6379
    assert agent_config.is_apple_silicon is False
    assert agent_config.sentiment_model_name == "sentimentdl_use_twitter"


@pytest.mark.asyncio
@patch("src.agent.main.RedisClientManager")
@patch("src.agent.main.SentimentExtract")
async def test_app_lifespan(mock_sentiment_class, mock_redis_manager_class):
    """Test FastAPI lifespan with mocked dependencies"""
    # Mock Redis client
    mock_redis_client = AsyncMock()
    mock_redis_manager_class.create = AsyncMock(return_value=mock_redis_client)
    
    # Mock sentiment service
    mock_sentiment = Mock()
    mock_sentiment_class.return_value = mock_sentiment
    
    # Import app
    from src.agent.main import app
    
    # Test client
    with TestClient(app) as client:
        # Test health endpoint
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}
    
    # Verify Redis client was created and closed
    mock_redis_manager_class.create.assert_called_once()
    mock_redis_client.close.assert_called_once()


def test_api_endpoints():
    """Test that API endpoints are registered"""
    from src.agent.main import app
    
    routes = [route.path for route in app.routes]
    
    assert "/health" in routes
    assert any(r.startswith("/api/feature_extraction") for r in routes)
    assert any(r.startswith("/api/sentiment") for r in routes)