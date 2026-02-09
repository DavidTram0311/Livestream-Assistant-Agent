"""Redis configuration with validation"""
from pydantic import Field, field_validator
from .base import BaseConfig

class RedisConfig(BaseConfig):
    """Redis connection configuration"""
    
    host: str = Field(
        default="localhost",
        description="Redis host",
        validation_alias="REDIS_HOST"
    )
    
    port: int = Field(
        default=6379,
        description="Redis port",
        validation_alias="REDIS_PORT"
    )
    
    db: int = Field(
        default=0,
        description="Redis database number",
        validation_alias="REDIS_DB"
    )
    
    password: str | None = Field(
        default=None,
        description="Redis password",
        validation_alias="REDIS_PASSWORD"
    )
    
    socket_timeout: int = Field(
        default=5,
        description="Socket timeout in seconds",
        validation_alias="REDIS_SOCKET_TIMEOUT"
    )
    
    socket_connect_timeout: int = Field(
        default=5,
        description="Socket connect timeout in seconds",
        validation_alias="REDIS_SOCKET_CONNECT_TIMEOUT"
    )
    
    decode_responses: bool = Field(
        default=True,
        description="Decode responses to strings",
        validation_alias="REDIS_DECODE_RESPONSES"
    )
    
    max_connections: int = Field(
        default=50,
        description="Maximum number of connections in pool",
        validation_alias="REDIS_MAX_CONNECTIONS"
    )
    
    @field_validator("port")
    def validate_port(cls, v):
        if not 1 <= v <= 65535:
            raise ValueError("port must be between 1 and 65535")
        return v
    
    @field_validator("db")
    def validate_db(cls, v):
        if not 0 <= v <= 15:
            raise ValueError("db must be between 0 and 15")
        return v
    
    def get_connection_kwargs(self) -> dict:
        """Get connection parameters for Redis client"""
        kwargs = {
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "decode_responses": self.decode_responses,
            "socket_timeout": self.socket_timeout,
            "socket_connect_timeout": self.socket_connect_timeout,
            "max_connections": self.max_connections,
        }
        if self.password:
            kwargs["password"] = self.password
        return kwargs
