export default function ArchitectureDiagram() {
  return (
    <svg viewBox="0 0 1000 500" className="w-full max-w-5xl mx-auto">
      <defs>
        <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
          <polygon points="0 0, 10 3.5, 0 7" fill="#0E4714" />
        </marker>
      </defs>

      {/* User Comments - Purple */}
      <g>
        <rect x="20" y="200" width="120" height="80" rx="8" fill="#9333ea" opacity="0.9" />
        <text x="80" y="235" textAnchor="middle" fill="white" fontSize="14" fontWeight="bold">User</text>
        <text x="80" y="255" textAnchor="middle" fill="white" fontSize="14" fontWeight="bold">Comments</text>
      </g>

      {/* PostgreSQL - Pink */}
      <g>
        <rect x="180" y="200" width="120" height="80" rx="8" fill="#ec4899" opacity="0.9" />
        <text x="240" y="235" textAnchor="middle" fill="white" fontSize="14" fontWeight="bold">PostgreSQL</text>
        <text x="240" y="255" textAnchor="middle" fill="white" fontSize="12">(CDC Source)</text>
      </g>

      {/* Kafka - Purple */}
      <g>
        <rect x="340" y="200" width="120" height="80" rx="8" fill="#9333ea" opacity="0.9" />
        <text x="400" y="235" textAnchor="middle" fill="white" fontSize="14" fontWeight="bold">Kafka</text>
        <text x="400" y="255" textAnchor="middle" fill="white" fontSize="12">(Debezium CDC)</text>
      </g>

      {/* Redis Feature Store - Pink */}
      <g>
        <rect x="420" y="80" width="100" height="60" rx="8" fill="#ec4899" opacity="0.9" />
        <text x="470" y="105" textAnchor="middle" fill="white" fontSize="12" fontWeight="bold">Redis</text>
        <text x="470" y="120" textAnchor="middle" fill="white" fontSize="10">(Feature Store)</text>
      </g>

      {/* Enrichment Service - Blue */}
      <g>
        <rect x="500" y="200" width="140" height="80" rx="8" fill="#3b82f6" opacity="0.9" />
        <text x="570" y="230" textAnchor="middle" fill="white" fontSize="13" fontWeight="bold">Enrichment</text>
        <text x="570" y="248" textAnchor="middle" fill="white" fontSize="11">SparkNLP +</text>
        <text x="570" y="264" textAnchor="middle" fill="white" fontSize="11">Gender Lookup</text>
      </g>

      {/* ksqlDB Aggregation - Blue */}
      <g>
        <rect x="680" y="185" width="120" height="110" rx="8" fill="#3b82f6" opacity="0.9" />
        <text x="740" y="210" textAnchor="middle" fill="white" fontSize="14" fontWeight="bold">ksqlDB</text>
        <text x="740" y="228" textAnchor="middle" fill="white" fontSize="9">(Sentiment joins</text>
        <text x="740" y="242" textAnchor="middle" fill="white" fontSize="9">user profile,</text>
        <text x="740" y="256" textAnchor="middle" fill="white" fontSize="9">aggregated over</text>
        <text x="740" y="270" textAnchor="middle" fill="white" fontSize="9">1min time window)</text>
      </g>

      {/* LLM Service - Orange */}
      <g>
        <rect x="840" y="200" width="140" height="80" rx="8" fill="#f97316" opacity="0.9" />
        <text x="910" y="230" textAnchor="middle" fill="white" fontSize="14" fontWeight="bold">GPT-4o</text>
        <text x="910" y="250" textAnchor="middle" fill="white" fontSize="12">(AI Insights)</text>
      </g>

      {/* Output - Orange */}
      <g>
        <rect x="840" y="320" width="140" height="60" rx="8" fill="#f97316" opacity="0.9" />
        <text x="910" y="345" textAnchor="middle" fill="white" fontSize="12" fontWeight="bold">Suggestions &</text>
        <text x="910" y="362" textAnchor="middle" fill="white" fontSize="12" fontWeight="bold">Recommendations</text>
      </g>

      {/* Arrows */}
      <line x1="140" y1="240" x2="175" y2="240" stroke="#0E4714" strokeWidth="2" markerEnd="url(#arrowhead)" />
      <line x1="300" y1="240" x2="335" y2="240" stroke="#0E4714" strokeWidth="2" markerEnd="url(#arrowhead)" />
      <line x1="460" y1="240" x2="495" y2="240" stroke="#0E4714" strokeWidth="2" markerEnd="url(#arrowhead)" />
      <line x1="640" y1="240" x2="675" y2="240" stroke="#0E4714" strokeWidth="2" markerEnd="url(#arrowhead)" />
      <line x1="800" y1="240" x2="835" y2="240" stroke="#0E4714" strokeWidth="2" markerEnd="url(#arrowhead)" />
      
      {/* Redis connection */}
      <line x1="470" y1="140" x2="540" y2="195" stroke="#0E4714" strokeWidth="2" strokeDasharray="5,5" markerEnd="url(#arrowhead)" />
      
      {/* Output arrow */}
      <line x1="910" y1="280" x2="910" y2="315" stroke="#0E4714" strokeWidth="2" markerEnd="url(#arrowhead)" />

      {/* Legend */}
      <g transform="translate(20, 420)">
        <rect x="0" y="0" width="20" height="20" rx="4" fill="#9333ea" opacity="0.9" />
        <text x="30" y="15" fill="#0E4714" fontSize="12">Real-time Events</text>
        
        <rect x="160" y="0" width="20" height="20" rx="4" fill="#ec4899" opacity="0.9" />
        <text x="190" y="15" fill="#0E4714" fontSize="12">External Storage</text>
        
        <rect x="320" y="0" width="20" height="20" rx="4" fill="#3b82f6" opacity="0.9" />
        <text x="350" y="15" fill="#0E4714" fontSize="12">Data Processing</text>
        
        <rect x="480" y="0" width="20" height="20" rx="4" fill="#f97316" opacity="0.9" />
        <text x="510" y="15" fill="#0E4714" fontSize="12">LLM Processing</text>
      </g>
    </svg>
  )
}
