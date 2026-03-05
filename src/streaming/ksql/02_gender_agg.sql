-- ============================================
-- Gender Distribution Aggregation (5-minute tumbling window)
-- ============================================
-- Aggregates gender distribution every 5 minutes.
-- Output: window_start, window_end, total_count, male_count, female_count, unknown_count
-- 
-- EMIT FINAL ensures we only emit results when the window closes,
-- providing accurate final counts rather than intermediate updates.

CREATE TABLE IF NOT EXISTS gender_stats 
WITH (
    KAFKA_TOPIC = 'streaming.gender_stats',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
) AS
SELECT
    AS_VALUE(WINDOWSTART) AS window_start,
    AS_VALUE(WINDOWEND) AS window_end,
    COUNT(*) AS total_count,
    SUM(CASE WHEN gender = 'male' THEN 1 ELSE 0 END) AS male_count,
    SUM(CASE WHEN gender = 'female' THEN 1 ELSE 0 END) AS female_count,
    SUM(CASE WHEN gender = 'unknown' OR gender IS NULL THEN 1 ELSE 0 END) AS unknown_count
FROM enriched_events
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY 1
EMIT FINAL;
