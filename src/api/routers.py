"""API routers for CDC Producer and Agent Services"""
from fastapi import APIRouter, HTTPException, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from pathlib import Path
from src.common.logging import get_logger
from src.cdc_producer.config import CDCProducerConfig
from src.cdc_producer.cdc_produce import produce_event, produce_event_timed
from src.api.models import (
    ProduceEventRequest, 
    ProduceEventResponse,
    ProduceEventTimedRequest,
    ProduceEventTimedResponse,
    SentimentRequest, 
    SentimentResponse,
    GenderResponse
)

logger = get_logger(__name__)

cdc_router = APIRouter()
feature_router = APIRouter()
sentiment_router = APIRouter()

@feature_router.get("/gender/{user_id}", response_model=GenderResponse)
async def get_gender_by_user(
    user_id: str,
    request: Request
):
    redis_client = request.app.state.redis_client
    
    try:
        gender = await redis_client.hget("user_genders", str(user_id))
    except Exception as e:
        logger.error(f"Redis error: {e}")
        raise HTTPException(status_code=500, detail="Redis error")

    if gender is None:
        raise HTTPException(status_code=404, detail="User not found")
    
    return GenderResponse(
        gender=gender,
        user_id=user_id
    )

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


@cdc_router.post("/produce/timed", response_model=ProduceEventTimedResponse)
async def produce_cdc_events_timed(
    request: ProduceEventTimedRequest,
    background_tasks: BackgroundTasks
):
    """
    Produce CDC events for a specified time window (background).
    
    Args:
        request: ProduceEventTimedRequest containing batch_size and time_window
        background_tasks: FastAPI background tasks
        
    Returns:
        ProduceEventTimedResponse with status and configuration
    """
    try:
        config = CDCProducerConfig()
        config.batch_size = request.batch_size
        
        logger.info(f"Starting timed CDC event production with batch_size={request.batch_size}, time_window={request.time_window}s")
        
        background_tasks.add_task(produce_event_timed, config, request.time_window)
        
        return ProduceEventTimedResponse(
            status="success",
            message=f"CDC event production started in background for {request.time_window} seconds",
            batch_size=request.batch_size,
            time_window=request.time_window
        )
        
    except Exception as e:
        logger.error(f"Error starting timed CDC event production: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start timed CDC event production: {str(e)}"
        )


@cdc_router.post("/produce/timed/sync", response_model=ProduceEventTimedResponse)
async def produce_cdc_events_timed_sync(request: ProduceEventTimedRequest):
    """
    Produce CDC events for a specified time window synchronously (blocking).
    
    Args:
        request: ProduceEventTimedRequest containing batch_size and time_window
        
    Returns:
        ProduceEventTimedResponse with status, configuration, and total records processed
    """
    try:
        config = CDCProducerConfig()
        config.batch_size = request.batch_size
        
        logger.info(f"Starting synchronous timed CDC event production with batch_size={request.batch_size}, time_window={request.time_window}s")
        
        total_records = produce_event_timed(config, request.time_window)
        
        return ProduceEventTimedResponse(
            status="success",
            message=f"CDC event production completed after {request.time_window} seconds",
            batch_size=request.batch_size,
            time_window=request.time_window,
            total_records_processed=total_records
        )
        
    except Exception as e:
        logger.error(f"Error in synchronous timed CDC event production: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Timed CDC event production failed: {str(e)}"
        )


@sentiment_router.post("/", response_model=SentimentResponse)
async def get_sentiment(
    payload: SentimentRequest,
    request: Request
):
    """
    Get sentiment prediction for the given text.
    
    Args:
        payload: SentimentRequest containing text and comment_id
        request: FastAPI Request object
        
    Returns:
        SentimentResponse with sentiment and confidence
    """
    sentiment_service = request.app.state.sentiment_service
    text = payload.text
    comment_id = payload.comment_id

    if text is None or comment_id is None:
        raise HTTPException(status_code=400, detail="Text and comment_id are required")
        
    try:
        sentiment = sentiment_service.predict(text)
    except Exception as e:
        logger.error(f"Sentiment error: {e}")
        raise HTTPException(status_code=500, detail="Sentiment error")
    
    return SentimentResponse(
        sentiment=sentiment,
        comment_id=comment_id
    )
