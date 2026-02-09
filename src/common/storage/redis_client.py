"""Redis client manager with connection pooling and retry logic"""
import redis.asyncio as redis
from redis.asyncio import Redis
from typing import Optional
from contextlib import asynccontextmanager
from ..config.redis_config import RedisConfig
from src.common.logging import get_logger

logger = get_logger(__name__)

class RedisClientManager:
    """
    Redis client manager with async support and connection pooling.
    
    Example:
        config = RedisConfig()
        client = await RedisClientManager.create(config)
        await client.set("key", "value")
        await client.close()
    """
    
    @staticmethod
    async def create(config: RedisConfig) -> Redis:
        """
        Create and validate Redis client.
        
        Args:
            config: RedisConfig instance
        
        Returns:
            Connected Redis client
        
        Raises:
            redis.ConnectionError: If connection fails
        """
        connection_kwargs = config.get_connection_kwargs()
        
        client = redis.Redis(**connection_kwargs)
        
        # Test connection
        try:
            await client.ping()
            logger.info(f"Successfully connected to Redis at {config.host}:{config.port}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            await client.close()
            raise
        
        return client
    
    @staticmethod
    @asynccontextmanager
    async def get_client(config: RedisConfig):
        """
        Context manager for Redis client.
        
        Example:
            async with RedisClientManager.get_client(config) as client:
                await client.set("key", "value")
        """
        client = await RedisClientManager.create(config)
        try:
            yield client
        finally:
            await client.close()
            logger.info("Redis connection closed")