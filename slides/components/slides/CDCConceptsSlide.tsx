import SlideContainer from '../SlideContainer'

export default function CDCConceptsSlide() {
  return (
    <SlideContainer>
      <h1 className="slide-title mb-8">Change Data Capture - Concepts</h1>
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="concept-card">
          <h2 className="slide-heading">What is CDC?</h2>
          <ul className="bullet-list text-lg">
            <li><span>Technique to track and capture data changes in a database</span></li>
            <li><span>Enables real-time data sync without full table scans</span></li>
          </ul>
        </div>

        <div className="concept-card">
          <h2 className="slide-heading">CDC Approaches</h2>
          <div className="space-y-3 slide-small">
            <div>
              <p className="font-bold text-slide-accent">Query-based:</p>
              <p>Polling with timestamps (simple but resource-intensive)</p>
            </div>
            <div>
              <p className="font-bold text-slide-accent">Log-based:</p>
              <p>Reading database transaction logs (efficient, low overhead)</p>
            </div>
          </div>
        </div>

        <div className="concept-card">
          <h2 className="slide-heading">Write-Ahead Log (WAL)</h2>
          <ul className="bullet-list text-lg">
            <li><span>Database recovery mechanism that logs changes before applying</span></li>
            <li><span>PostgreSQL: <code className="bg-slate-200 px-2 py-1 rounded">wal_level=logical</code> enables logical decoding</span></li>
          </ul>
        </div>

        <div className="concept-card">
          <h2 className="slide-heading">Debezium</h2>
          <ul className="bullet-list text-lg">
            <li><span>Open-source CDC platform built on Kafka Connect</span></li>
            <li><span>Supports PostgreSQL, MySQL, MongoDB, SQL Server</span></li>
            <li><span>Produces change events in Avro/JSON format</span></li>
          </ul>
        </div>
      </div>
    </SlideContainer>
  )
}
