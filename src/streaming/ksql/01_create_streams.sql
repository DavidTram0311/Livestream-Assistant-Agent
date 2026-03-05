-- ============================================
-- Create Stream from Enriched Events Topic
-- ============================================
-- This stream reads from the enriched events topic produced by the Python enrichment service.
-- The event_timestamp field is used as the message timestamp for windowing operations.

CREATE STREAM IF NOT EXISTS enriched_events (
    comment_id BIGINT,
    user_id VARCHAR,
    comments VARCHAR,
    gender VARCHAR,
    sentiment VARCHAR,
    event_timestamp BIGINT
) WITH (
    KAFKA_TOPIC = 'streaming.enriched_events',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'event_timestamp'
);
