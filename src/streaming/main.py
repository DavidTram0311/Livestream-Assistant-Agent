"""
Streaming Enrichment Service Entry Point

This service:
1. Consumes CDC events from tracking_postgres_cdc.public.comment_events
2. Enriches events with gender (from Redis via API) and sentiment (from ML model via API)
3. Produces enriched events to streaming.enriched_events topic

The enriched events are then processed by ksqlDB for 5-minute windowed aggregation.
"""
import asyncio
import signal
import sys
from src.common.logging import setup_logging, get_logger
from src.streaming.config import StreamingConfig
from src.streaming.enrichment.processor import EnrichmentProcessor

setup_logging(level="INFO")
logger = get_logger(__name__)


async def main():
    """Main entry point for the streaming enrichment service"""
    logger.info("=" * 60)
    logger.info("Starting Streaming Enrichment Service")
    logger.info("=" * 60)
    
    # Load configuration
    config = StreamingConfig()
    logger.info(f"Input topic: {config.input_topic}")
    logger.info(f"Output topic: {config.output_topic}")
    logger.info(f"API base URL: {config.api_base_url}")
    logger.info(f"Kafka bootstrap servers: {config.kafka_bootstrap_servers}")
    
    # Create processor
    processor = EnrichmentProcessor(config)
    
    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        logger.info("Received shutdown signal")
        processor.stop()
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        await processor.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        logger.info("Streaming Enrichment Service stopped")


if __name__ == "__main__":
    asyncio.run(main())
