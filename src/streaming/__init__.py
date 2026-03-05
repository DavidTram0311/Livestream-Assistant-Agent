"""
Streaming Enrichment Service

This module provides a Kafka-based streaming pipeline that:
1. Consumes CDC events from tracking_postgres_cdc.public.comment_events
2. Enriches events with gender and sentiment via HTTP API calls
3. Produces enriched events to streaming.enriched_events topic

The enriched events are then processed by ksqlDB for 5-minute windowed aggregation,
producing statistics for LLM consumption.
"""
from .config import StreamingConfig
from .models import CDCEvent, EnrichedEvent, DebeziumEnvelope

__all__ = [
    "StreamingConfig",
    "CDCEvent",
    "EnrichedEvent",
    "DebeziumEnvelope",
]
