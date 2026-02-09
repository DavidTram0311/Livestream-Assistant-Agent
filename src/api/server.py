"""CDC Producer API Server"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from common.logging import setup_logging, get_logger
from api.routers import cdc_router

# Setup logging
setup_logging(level="INFO")
logger = get_logger(__name__)

# Create FastAPI app
app = FastAPI(
    title="CDC Producer API",
    description="API for producing CDC events from parquet files to PostgreSQL",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    """Root endpoint"""
    return JSONResponse(
        status_code=200,
        content={
            "service": "CDC Producer API",
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


# Include CDC router
app.include_router(
    cdc_router,
    prefix="/api/cdc",
    tags=["cdc"],
)


def main():
    """Main entry point"""
    logger.info("Starting CDC Producer API server...")
    uvicorn.run(
        "api.server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=1
    )


if __name__ == "__main__":
    main()
