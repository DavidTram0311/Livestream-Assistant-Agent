import SlideContainer from '../SlideContainer'

export default function EnrichmentImplSlide() {
  return (
    <SlideContainer>
      <h1 className="slide-title mb-6">Stream Enrichment - Implementation</h1>
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="concept-card">
          <h2 className="slide-heading mb-4">This Project&apos;s Flow</h2>
          <div className="slide-code text-xs">
            <pre>{`Enrichment Service (Python)
    └── Consumes: tracking_postgres_cdc
                  .public.comment_events
            │
            ├──▶ Redis: Gender lookup by user_id
            │       └── hget("user_genders", user_id)
            │
            └──▶ SparkNLP: Sentiment analysis
                    └── sentimentdl_use_twitter
            │
            ▼
Kafka Topic: streaming.enriched_events
    └── comment_id, user_id, comments,
        gender, sentiment, event_timestamp`}</pre>
          </div>
        </div>

        <div className="space-y-6">
          <div className="concept-card">
            <h2 className="slide-heading mb-4">SparkNLP Pipeline</h2>
            <ol className="space-y-2 slide-small">
              <li className="flex gap-2">
                <span className="font-bold text-slide-accent">1.</span>
                <span><strong>DocumentAssembler</strong> → Convert text to document</span>
              </li>
              <li className="flex gap-2">
                <span className="font-bold text-slide-accent">2.</span>
                <span><strong>UniversalSentenceEncoder</strong> → Generate embeddings</span>
              </li>
              <li className="flex gap-2">
                <span className="font-bold text-slide-accent">3.</span>
                <span><strong>SentimentDLModel</strong> → Classify positive/negative</span>
              </li>
            </ol>
          </div>

          <div className="concept-card">
            <h2 className="slide-heading mb-4">Tech Stack</h2>
            <ul className="bullet-list text-lg">
              <li><span><strong>confluent-kafka</strong> (Python client)</span></li>
              <li><span><strong>Redis</strong> with async support</span></li>
              <li><span><strong>SparkNLP 6.2.3</strong> + PySpark 3.5.1</span></li>
            </ul>
          </div>
        </div>
      </div>
    </SlideContainer>
  )
}
