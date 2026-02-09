"""Pydantic models for CDC Producer API"""
from pydantic import BaseModel, Field
from typing import Optional


class ProduceEventRequest(BaseModel):
    """Request model for producing CDC events"""
    batch_size: Optional[int] = Field(
        default=10,
        description="Batch size for event production",
        ge=1,
        le=1000
    )
    max_records: Optional[int] = Field(
        default=100,
        description="Maximum records to process",
        ge=1,
        le=10000
    )


class ProduceEventResponse(BaseModel):
    """Response model for produce event endpoint"""
    status: str
    message: str
    batch_size: int
    max_records: int
