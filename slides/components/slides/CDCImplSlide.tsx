import SlideContainer from '../SlideContainer'
import CDCDiagram from '../diagrams/CDCDiagram'

export default function CDCImplSlide() {
  return (
    <SlideContainer>
      <h1 className="slide-title mb-6">CDC - Implementation</h1>
      
      <CDCDiagram />
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-8">
        <div className="concept-card">
          <h2 className="slide-heading mb-4">Debezium Envelope Structure</h2>
          <div className="slide-code text-sm">
            <pre>{`{
  "before": null,      // Previous state
  "after": {...},      // New row state
  "op": "c",           // c=create, u=update, d=delete
  "ts_ms": 1709123456  // Capture timestamp
}`}</pre>
          </div>
        </div>

        <div className="concept-card">
          <h2 className="slide-heading mb-4">Tech Stack</h2>
          <ul className="bullet-list text-lg">
            <li><span><strong>Kafka Connect</strong> (distributed mode)</span></li>
            <li><span><strong>Debezium</strong> PostgreSQL Connector</span></li>
            <li><span><strong>Schema Registry</strong> (Avro schemas)</span></li>
            <li><span><strong>3-node Kafka</strong> cluster (KRaft mode)</span></li>
          </ul>
        </div>
      </div>
    </SlideContainer>
  )
}
