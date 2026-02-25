import os
import pyarrow.parquet as pq
import logging
from redis.asyncio import Redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default path if not provided
DEFAULT_PARQUET_PATH = os.path.join(os.path.dirname(__file__), "data", "user_table.parquet")

async def push_parquet_to_redis(redis_client: Redis, parquet_path: str = DEFAULT_PARQUET_PATH):
    """
    Push user data from parquet to Redis using hset.
    """
    try: 
        # 1. Check if the parquet file exists
        if not os.path.exists(parquet_path):
            logging.error(f"Parquet file not found: {parquet_path}")
            return

        # 2. Read the parquet file
        table = pq.ParquetFile(parquet_path)

        # 3. Use pipeline to push the parquet file to Redis
        pipe = redis_client.pipeline()

        # 4. Push the parquet file to Redis
        total_rows = 0
        for batch in table.iter_batches(batch_size=10000):
            batch_df = batch.to_pandas()

            mapping = dict(zip(batch_df['reviewerID'].astype(str), batch_df['gender'].astype(str)))
            # We use mapping for hset in redis-py >= 3.0.0
            pipe.hset("user_genders", mapping=mapping)

            await pipe.execute()
            total_rows += len(batch_df)
            logging.info(f"Pushed {total_rows} rows to Redis")

        logging.info(f"Successfully pushed all {total_rows} rows to Redis")

    except Exception as e:
        logging.error(f"Error pushing parquet file to Redis: {e}")
        raise e

if __name__ == "__main__":
    # For testing purposes if needed
    import asyncio
    from src.agent.config import AgentConfig
    from src.common.storage import RedisClientManager
    
    async def test():
        config = AgentConfig()
        redis_client = await RedisClientManager.create(config)
        await push_parquet_to_redis(redis_client)
        await redis_client.close()

    asyncio.run(test())
