import SlideContainer from '../SlideContainer'

export default function EnrichmentConceptsSlide() {
  return (
    <SlideContainer>
      <h1 className="slide-title mb-8">Stream Enrichment - Concepts</h1>
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="concept-card">
          <h2 className="slide-heading">Feature Stores</h2>
          <ul className="bullet-list text-lg">
            <li><span>Centralized repository for ML features</span></li>
            <li><span>Enables feature reuse and consistency</span></li>
            <li><span>Types: <strong>Online</strong> (low-latency lookup), <strong>Offline</strong> (batch training)</span></li>
          </ul>
        </div>

        <div className="concept-card">
          <h2 className="slide-heading">Redis as Online Feature Store</h2>
          <ul className="bullet-list text-lg">
            <li><span>In-memory key-value store</span></li>
            <li><span>Sub-millisecond read latency</span></li>
            <li><span>Hash data structure for user profiles</span></li>
          </ul>
        </div>

        <div className="concept-card">
          <h2 className="slide-heading">NLP Pipelines</h2>
          <ul className="bullet-list text-lg">
            <li><span>Text preprocessing → Embedding → Classification</span></li>
            <li><span>Transfer learning with pre-trained models</span></li>
          </ul>
        </div>

        <div className="concept-card">
          <h2 className="slide-heading">Spark NLP</h2>
          <ul className="bullet-list text-lg">
            <li><span>Open-source NLP library built on Apache Spark</span></li>
            <li><span>Pre-trained models: Sentiment, NER, Classification</span></li>
            <li><span>LightPipeline for single-record inference</span></li>
          </ul>
        </div>
      </div>
    </SlideContainer>
  )
}
