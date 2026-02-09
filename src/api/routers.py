"""API routers for CDC Producer"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pathlib import Path
from common.logging import get_logger
from cdc_producer.config import CDCProducerConfig
from cdc_producer.cdc_produce import produce_event
from api.models import ProduceEventRequest, ProduceEventResponse

logger = get_logger(__name__)

cdc_router = APIRouter()


@cdc_router.post("/produce", response_model=ProduceEventResponse)
async def produce_cdc_events(
    request: ProduceEventRequest,
    background_tasks: BackgroundTasks
):
    """
    Produce CDC events from parquet file to PostgreSQL.
    
    Args:
        request: ProduceEventRequest containing batch_size and max_records
        background_tasks: FastAPI background tasks
        
    Returns:
        ProduceEventResponse with status and configuration
    """
    try:
        # Load configuration
        config = CDCProducerConfig()
        config.batch_size = request.batch_size
        config.max_records = request.max_records
        
        logger.info(f"Starting CDC event production with batch_size={request.batch_size}, max_records={request.max_records}")
        
        # Run produce_event in background
        background_tasks.add_task(produce_event, config)
        
        return ProduceEventResponse(
            status="success",
            message="CDC event production started in background",
            batch_size=request.batch_size,
            max_records=request.max_records
        )
        
    except Exception as e:
        logger.error(f"Error starting CDC event production: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start CDC event production: {str(e)}"
        )


@cdc_router.post("/produce/sync", response_model=ProduceEventResponse)
async def produce_cdc_events_sync(request: ProduceEventRequest):
    """
    Produce CDC events synchronously (blocking).
    
    Args:
        request: ProduceEventRequest containing batch_size and max_records
        
    Returns:
        ProduceEventResponse with status and configuration
    """
    try:
        # Load configuration
        config = CDCProducerConfig()
        config.batch_size = request.batch_size
        config.max_records = request.max_records
        
        logger.info(f"Starting synchronous CDC event production with batch_size={request.batch_size}, max_records={request.max_records}")
        
        # Run produce_event synchronously
        produce_event(config)
        
        return ProduceEventResponse(
            status="success",
            message="CDC event production completed successfully",
            batch_size=request.batch_size,
            max_records=request.max_records
        )
        
    except Exception as e:
        logger.error(f"Error in synchronous CDC event production: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"CDC event production failed: {str(e)}"
        )
