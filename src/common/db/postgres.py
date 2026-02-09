"""PostgreSQL client with connection pooling and improved error handling"""
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from typing import Optional
from contextlib import contextmanager
from ..config.postgres_config import PostgresConfig
from src.common.logging import get_logger

logger = get_logger(__name__)

class PostgresClient:
    """
    PostgreSQL client with connection pooling and session management.
    
    Example:
        config = PostgresConfig()
        client = PostgresClient(config)
        
        with client.get_session() as session:
            # Use session
            pass
    """
    
    def __init__(self, config: PostgresConfig):
        """
        Initialize PostgreSQL client.
        
        Args:
            config: PostgresConfig instance
        """
        self.config = config
        self.engine = None
        self.SessionLocal = None
        self._connect()
    
    def _connect(self):
        """Establish database connection with pooling"""
        connection_url = self.config.get_connection_url(hide_password=False)
        
        # Log connection (without password)
        logger.info(f"Connecting to PostgreSQL: {self.config.get_connection_url(hide_password=True)}")
        
        # Create engine with connection pooling
        self.engine = create_engine(
            connection_url,
            poolclass=QueuePool,
            **self.config.get_engine_kwargs()
        )
        
        # Create session factory
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        
        # Test connection
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("PostgreSQL connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    @contextmanager
    def get_session(self) -> Session:
        """
        Get database session with automatic cleanup.
        
        Yields:
            SQLAlchemy Session
        
        Example:
            with client.get_session() as session:
                session.query(Model).all()
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Session error: {e}")
            raise
        finally:
            session.close()
    
    def create_tables(self, base):
        """
        Create all tables for a declarative base.
        
        Args:
            base: SQLAlchemy declarative base
        """
        base.metadata.create_all(self.engine)
        logger.info("Database tables created")
    
    def drop_tables(self, base):
        """
        Drop all tables for a declarative base.
        
        Args:
            base: SQLAlchemy declarative base
        """
        base.metadata.drop_all(self.engine)
        logger.info("Database tables dropped")
    
    def close(self):
        """Close all connections and dispose engine"""
        if self.engine:
            self.engine.dispose()
            logger.info("PostgreSQL connection pool disposed")