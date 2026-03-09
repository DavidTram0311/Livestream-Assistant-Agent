import SlideContainer from '../SlideContainer'

export default function DataIngestionConceptsSlide() {
  return (
    <SlideContainer>
      <h1 className="slide-title mb-10">Data Ingestion - Concepts</h1>
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <div className="concept-card">
          <h2 className="slide-heading">What is Data Ingestion?</h2>
          <ul className="bullet-list text-lg">
            <li><span>The process of importing data from various sources into a storage system</span></li>
            <li><span>Key considerations: <strong>Volume</strong>, <strong>Velocity</strong>, <strong>Variety</strong></span></li>
          </ul>
        </div>

        <div className="concept-card">
          <h2 className="slide-heading">Batch vs Streaming</h2>
          <div className="space-y-4 slide-small">
            <div>
              <p className="font-bold text-slide-accent">Batch:</p>
              <p>Periodic bulk loads (hourly/daily), higher latency, simpler</p>
            </div>
            <div>
              <p className="font-bold text-slide-accent">Streaming:</p>
              <p>Continuous real-time flow, low latency, more complex</p>
            </div>
          </div>
        </div>

        <div className="concept-card lg:col-span-2">
          <h2 className="slide-heading">Apache Parquet</h2>
          <ul className="bullet-list text-lg">
            <li><span>Columnar storage format optimized for analytics</span></li>
            <li><span>Efficient compression and encoding</span></li>
            <li><span>Schema evolution support</span></li>
          </ul>
        </div>
      </div>
    </SlideContainer>
  )
}
