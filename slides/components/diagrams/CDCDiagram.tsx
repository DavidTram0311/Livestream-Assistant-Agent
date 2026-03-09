export default function CDCDiagram() {
  return (
    <svg viewBox="0 0 600 200" className="w-full max-w-3xl mx-auto">
      <defs>
        <marker id="cdc-arrow" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
          <polygon points="0 0, 10 3.5, 0 7" fill="#0E4714" />
        </marker>
      </defs>

      {/* PostgreSQL WAL */}
      <g>
        <rect x="20" y="60" width="140" height="80" rx="8" fill="#3b82f6" opacity="0.9" />
        <text x="90" y="95" textAnchor="middle" fill="white" fontSize="14" fontWeight="bold">PostgreSQL</text>
        <text x="90" y="115" textAnchor="middle" fill="white" fontSize="12">WAL (logical)</text>
      </g>

      {/* Debezium */}
      <g>
        <rect x="220" y="60" width="160" height="80" rx="8" fill="#ec4899" opacity="0.9" />
        <text x="300" y="90" textAnchor="middle" fill="white" fontSize="14" fontWeight="bold">Kafka Connect</text>
        <text x="300" y="110" textAnchor="middle" fill="white" fontSize="12">+ Debezium</text>
        <text x="300" y="128" textAnchor="middle" fill="white" fontSize="11">Connector</text>
      </g>

      {/* Kafka Topic */}
      <g>
        <rect x="440" y="60" width="140" height="80" rx="8" fill="#9333ea" opacity="0.9" />
        <text x="510" y="90" textAnchor="middle" fill="white" fontSize="14" fontWeight="bold">Kafka Topic</text>
        <text x="510" y="110" textAnchor="middle" fill="white" fontSize="10">tracking_postgres_cdc</text>
        <text x="510" y="125" textAnchor="middle" fill="white" fontSize="10">.public.comment_events</text>
      </g>

      {/* Arrows */}
      <line x1="160" y1="100" x2="215" y2="100" stroke="#0E4714" strokeWidth="2" markerEnd="url(#cdc-arrow)" />
      <line x1="380" y1="100" x2="435" y2="100" stroke="#0E4714" strokeWidth="2" markerEnd="url(#cdc-arrow)" />

      {/* Labels */}
      <text x="188" y="85" textAnchor="middle" fill="#0E4714" fontSize="10">reads</text>
      <text x="408" y="85" textAnchor="middle" fill="#0E4714" fontSize="10">produces</text>
    </svg>
  )
}
