-- Step 1: Drop the existing table (and its underlying topic if needed)
-- DROP TABLE IF EXISTS combined_stats DELETE TOPIC;

-- Step 2: Recreate with the new window size
CREATE TABLE IF NOT EXISTS combined_stats 
WITH (
    KAFKA_TOPIC = 'streaming.combined_stats',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
) AS
SELECT
    'all' AS agg_key,
    AS_VALUE(WINDOWSTART) AS window_start,
    AS_VALUE(WINDOWEND) AS window_end,
    COUNT(*) AS total_count,
    SUM(CASE WHEN gender = 'male' THEN 1 ELSE 0 END) AS male_count,
    SUM(CASE WHEN gender = 'female' THEN 1 ELSE 0 END) AS female_count,
    SUM(CASE WHEN gender = 'unknown' OR gender IS NULL THEN 1 ELSE 0 END) AS gender_unknown_count,
    SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_count,
    SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_count,
    SUM(CASE WHEN sentiment = 'unknown' OR sentiment IS NULL OR sentiment = 'empty_text' THEN 1 ELSE 0 END) AS sentiment_unknown_count
FROM enriched_events
WINDOW TUMBLING (SIZE 1 MINUTES)
GROUP BY 'all'
EMIT FINAL;