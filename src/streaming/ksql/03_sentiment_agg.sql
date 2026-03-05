-- ============================================
-- Sentiment Distribution Aggregation (5-minute tumbling window)
-- ============================================
-- Aggregates sentiment distribution every 5 minutes.
-- Output: window_start, window_end, total_count, positive_count, negative_count, unknown_count
--
-- EMIT FINAL ensures we only emit results when the window closes,
-- providing accurate final counts rather than intermediate updates.

CREATE TABLE IF NOT EXISTS sentiment_stats 
WITH (
    KAFKA_TOPIC = 'streaming.sentiment_stats',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
) AS
SELECT
    AS_VALUE(WINDOWSTART) AS window_start,
    AS_VALUE(WINDOWEND) AS window_end,
    COUNT(*) AS total_count,
    SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_count,
    SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_count,
    SUM(CASE WHEN sentiment = 'unknown' OR sentiment IS NULL OR sentiment = 'empty_text' THEN 1 ELSE 0 END) AS unknown_count
FROM enriched_events
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY 1
EMIT FINAL;
