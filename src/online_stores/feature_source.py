from dotenv import load_dotenv
import os
import pyarrow.parquet as pq
import redis
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
PARQUET_PATH = os.path.join(os.path.dirname(__file__), "data", "user_table.parquet")

def push_parquet_to_redis(parquet_path: str, redis_host: str, redis_port: int):
    try: 
        # 1. Connect to Redis
        redis_client = redis.Redis(host=redis_host, port=redis_port)

        # 2. Read the parquet file
        table = pq.ParquetFile(parquet_path)

        # 3. Use pipeline to push the parquet file to Redis
        pipe = redis_client.pipeline()

        # 4. Push the parquet file to Redis
        total_rows = 0
        for batch in table.iter_batches(batch_size=10000):
            # if total_rows == 10000:
            #     logging.info(f"Pushed {total_rows} rows to Redis")
            #     break

            batch_df = batch.to_pandas()

            mapping = dict(zip(batch_df['reviewerID'].astype(str), batch_df['gender'].astype(str)))
            pipe.hset("user_genders", mapping=mapping)

            pipe.execute()
            total_rows += len(batch_df)
            logging.info(f"Pushed {total_rows} rows to Redis")

        logging.info(f"Pushed all {total_rows} rows to Redis")

    except Exception as e:
        logging.error(f"Error pushing parquet file to Redis: {e}")
        raise e

if __name__ == "__main__":
    push_parquet_to_redis(PARQUET_PATH, REDIS_HOST, REDIS_PORT)

