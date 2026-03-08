"""
LLM Insight Extraction Service Entry Point

This service:
1. Consumes combined gender and sentiment stats from ksqlDB topic
2. Generates insights using OpenAI
3. Produces insights to streaming.llm_insights topic
"""
import asyncio
import signal
import sys
from src.common.logging import setup_logging, get_logger
from src.common.config import LLMInsightConfig
from src.streaming.llm_insight.processor import LLMInsightProcessor

setup_logging(level="INFO")
logger = get_logger(__name__)


async def main():
    """Main entry point for the LLM Insight service"""
    logger.info("=" * 60)
    logger.info("Starting LLM Insight Extraction Service")
    logger.info("=" * 60)
    
    config = LLMInsightConfig()
    logger.info(f"Combined stats topic: {config.combined_stats_topic}")
    logger.info(f"Output topic: {config.output_topic}")
    logger.info(f"OpenAI model: {config.openai_model}")
    logger.info(f"Kafka bootstrap servers: {config.kafka_bootstrap_servers}")
    
    if not config.openai_api_key:
        logger.warning("OPENAI_API_KEY not set - using fallback insight generation")
    
    processor = LLMInsightProcessor(config)
    
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
        logger.info("LLM Insight Extraction Service stopped")


if __name__ == "__main__":
    asyncio.run(main())
