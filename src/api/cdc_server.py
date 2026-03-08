"""CDC Producer API Server - Lightweight service for CDC endpoints only"""
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from src.common.logging import setup_logging, get_logger
from src.api.routers import cdc_router
from src.cdc_producer.config import CDCProducerConfig
from src.common.db import PostgresClient
from src.cdc_producer.db import Base

setup_logging(level="INFO")
logger = get_logger(__name__)

app = FastAPI(
    title="CDC Producer API Service",
    description="Lightweight API service for CDC producer endpoints",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Initialize database tables on startup"""
    try:
        logger.info("Initializing database tables...")
        config = CDCProducerConfig()
        pg_client = PostgresClient(config)
        
        # Create tables if they don't exist
        pg_client.create_tables(Base)
        logger.info("✅ Database tables initialized successfully")
        
        # Close the client after table creation
        pg_client.close()
    except Exception as e:
        logger.error(f"Failed to initialize database tables: {e}")
        raise


@app.get("/")
def root():
    """Root endpoint"""
    return JSONResponse(
        status_code=200,
        content={
            "service": "CDC Producer API Service",
            "version": "1.0.0",
            "status": "running"
        }
    )


@app.get("/health")
def health():
    """Health check endpoint"""
    return JSONResponse(
        status_code=200,
        content={"status": "healthy"}
    )


app.include_router(
    cdc_router,
    prefix="/api/cdc",
    tags=["cdc"],
)


def main():
    """Main entry point"""
    port = int(os.getenv("CDC_API_PORT", "8001"))
    logger.info(f"Starting CDC Producer API Service on port {port}...")
    uvicorn.run(
        "src.api.cdc_server:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info"
    )


if __name__ == "__main__":
    main()
