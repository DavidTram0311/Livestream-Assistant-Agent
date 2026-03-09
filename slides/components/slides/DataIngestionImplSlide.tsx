import SlideContainer from '../SlideContainer'

export default function DataIngestionImplSlide() {
  return (
    <SlideContainer>
      <h1 className="slide-title mb-8">Data Ingestion - Implementation</h1>
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <div className="concept-card">
          <h2 className="slide-heading mb-6">This Project&apos;s Flow</h2>
          <div className="slide-code text-sm">
            <pre>{`Parquet Files (Amazon Reviews)
    └── reviewerID, reviewText
            │
            ▼
CDC Producer API (POST /api/cdc/produce)
            │
            ▼
PostgreSQL (comment_events table)
    └── comment_id, user_id, comments, 
        event_timestamp`}</pre>
          </div>
        </div>

        <div className="concept-card">
          <h2 className="slide-heading mb-6">Tech Stack</h2>
          <ul className="bullet-list">
            <li><span><strong>PyArrow</strong> for Parquet reading</span></li>
            <li><span><strong>FastAPI</strong> for CDC Producer API</span></li>
            <li><span><strong>PostgreSQL 15</strong> as source database</span></li>
            <li><span><strong>SQLAlchemy</strong> ORM</span></li>
          </ul>
        </div>
      </div>
    </SlideContainer>
  )
}
