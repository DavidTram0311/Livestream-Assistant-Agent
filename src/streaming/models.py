"""Pydantic models for streaming service"""
from pydantic import BaseModel, Field
from typing import Optional


class CDCEvent(BaseModel):
    """
    Input event from CDC topic (tracking_postgres_cdc.public.comment_events).
    This represents the Debezium CDC payload after extraction.
    """
    comment_id: int = Field(..., description="Unique comment identifier")
    user_id: str = Field(..., description="User identifier")
    comments: str = Field(..., description="Comment text content")
    event_timestamp: int = Field(..., description="Event timestamp in epoch milliseconds")


class EnrichedEvent(BaseModel):
    """
    Enriched event with gender and sentiment information.
    Published to streaming.enriched_events topic.
    """
    comment_id: int = Field(..., description="Unique comment identifier")
    user_id: str = Field(..., description="User identifier")
    comments: str = Field(..., description="Comment text content")
    gender: str = Field(default="unknown", description="User gender (male/female/unknown)")
    sentiment: str = Field(default="unknown", description="Comment sentiment (positive/negative/unknown)")
    event_timestamp: int = Field(..., description="Event timestamp in epoch milliseconds")
    
    @classmethod
    def from_cdc_event(
        cls, 
        cdc_event: CDCEvent, 
        gender: str = "unknown", 
        sentiment: str = "unknown"
    ) -> "EnrichedEvent":
        """Create an EnrichedEvent from a CDCEvent with enrichment data"""
        return cls(
            comment_id=cdc_event.comment_id,
            user_id=cdc_event.user_id,
            comments=cdc_event.comments,
            gender=gender,
            sentiment=sentiment,
            event_timestamp=cdc_event.event_timestamp
        )


class GenderAPIResponse(BaseModel):
    """Response model from gender API endpoint"""
    gender: str
    user_id: str


class SentimentAPIRequest(BaseModel):
    """Request model for sentiment API endpoint"""
    text: str
    comment_id: int


class SentimentAPIResponse(BaseModel):
    """Response model from sentiment API endpoint"""
    sentiment: str
    comment_id: int


class DebeziumEnvelope(BaseModel):
    """
    Debezium CDC envelope structure.
    The actual data is nested under 'after' for insert/update operations.
    """
    before: Optional[dict] = None
    after: Optional[dict] = None
    source: Optional[dict] = None
    op: Optional[str] = None
    ts_ms: Optional[int] = None
    
    def extract_event(self) -> Optional[CDCEvent]:
        """Extract CDCEvent from Debezium envelope"""
        if self.after is None:
            return None
        
        return CDCEvent(
            comment_id=self.after.get("comment_id"),
            user_id=self.after.get("user_id", ""),
            comments=self.after.get("comments", ""),
            event_timestamp=self.after.get("event_timestamp", self.ts_ms or 0)
        )
