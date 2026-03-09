import SlideContainer from '../SlideContainer'

export default function AggregationImplSlide() {
  return (
    <SlideContainer>
      <h1 className="slide-title mb-6">Real-Time Aggregation - Implementation</h1>
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="concept-card">
          <h2 className="slide-heading mb-4">This Project Uses</h2>
          <ul className="bullet-list text-lg">
            <li><span><strong>Window:</strong> 1-minute Tumbling Window</span></li>
            <li><span><strong>Time:</strong> Event Time (<code className="bg-slate-200 px-1 rounded text-sm">event_timestamp</code>)</span></li>
            <li><span><strong>Grace Period:</strong> 0 seconds (late events dropped)</span></li>
          </ul>

          <h2 className="slide-heading mt-6 mb-4">ksqlDB Concepts</h2>
          <ul className="bullet-list text-lg">
            <li><span><strong>Streams:</strong> Unbounded events (immutable)</span></li>
            <li><span><strong>Tables:</strong> Materialized view with latest state</span></li>
            <li><span><strong>EMIT FINAL:</strong> Emit only when window closes</span></li>
          </ul>
        </div>

        <div className="concept-card">
          <h2 className="slide-heading mb-4">ksqlDB Query</h2>
          <div className="slide-code text-xs overflow-x-auto">
            <pre>{`CREATE TABLE combined_stats AS
SELECT
    'all' AS agg_key,
    COUNT(*) AS total_count,
    SUM(CASE WHEN gender = 'male' 
        THEN 1 ELSE 0 END) AS male_count,
    SUM(CASE WHEN sentiment = 'positive' 
        THEN 1 ELSE 0 END) AS positive_count,
    ...
FROM enriched_events
WINDOW TUMBLING (SIZE 1 MINUTES)
GROUP BY 'all'
EMIT FINAL;`}</pre>
          </div>
          <p className="slide-small mt-4">
            <strong>Output Topic:</strong>{' '}
            <code className="bg-slate-200 px-2 py-1 rounded text-sm">streaming.combined_stats</code>
          </p>
        </div>
      </div>
    </SlideContainer>
  )
}
