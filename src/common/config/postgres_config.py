"""PostgreSQL configuration with validation"""
from pydantic import Field, field_validator
from .base import BaseConfig


class PostgresConfig(BaseConfig):
    """PostgreSQL connection configuration"""
    
    host: str = Field(
        default="localhost",
        description="PostgreSQL host",
        validation_alias="POSTGRES_HOST"
    )
    
    port: int = Field(
        default=5432,
        description="PostgreSQL port",
        validation_alias="POSTGRES_PORT"
    )
    
    database: str = Field(
        ...,
        description="PostgreSQL database name",
        validation_alias="POSTGRES_DB"
    )
    
    user: str = Field(
        ...,
        description="PostgreSQL user",
        validation_alias="POSTGRES_USER"
    )
    
    password: str = Field(
        ...,
        description="PostgreSQL password",
        validation_alias="POSTGRES_PASSWORD"
    )
    
    # Connection pool settings
    pool_size: int = Field(
        default=10,
        description="Connection pool size",
        validation_alias="POSTGRES_POOL_SIZE"
    )
    
    max_overflow: int = Field(
        default=20,
        description="Maximum overflow connections",
        validation_alias="POSTGRES_MAX_OVERFLOW"
    )
    
    pool_recycle: int = Field(
        default=3600,
        description="Connection recycle time in seconds",
        validation_alias="POSTGRES_POOL_RECYCLE"
    )
    
    pool_pre_ping: bool = Field(
        default=True,
        description="Enable connection health checks",
        validation_alias="POSTGRES_POOL_PRE_PING"
    )
    
    echo: bool = Field(
        default=False,
        description="Echo SQL statements",
        validation_alias="POSTGRES_ECHO"
    )
    
    @field_validator("port")
    def validate_port(cls, v):
        if not 1 <= v <= 65535:
            raise ValueError("port must be between 1 and 65535")
        return v
    
    @field_validator("pool_size")
    def validate_pool_size(cls, v):
        if v < 1:
            raise ValueError("pool_size must be at least 1")
        return v
    
    def get_connection_url(self, hide_password: bool = False) -> str:
        """Get PostgreSQL connection URL"""
        password = "***" if hide_password else self.password
        return f"postgresql://{self.user}:{password}@{self.host}:{self.port}/{self.database}"
    
    def get_engine_kwargs(self) -> dict:
        """Get SQLAlchemy engine parameters"""
        return {
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_recycle": self.pool_recycle,
            "pool_pre_ping": self.pool_pre_ping,
            "echo": self.echo,
        }
