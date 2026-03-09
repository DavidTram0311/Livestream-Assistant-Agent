export default function TimeNotionsDiagram() {
  return (
    <svg viewBox="0 0 500 200" className="w-full max-w-2xl mx-auto">
      {/* Event Source */}
      <g>
        <rect x="20" y="80" width="80" height="50" rx="6" fill="#9333ea" opacity="0.9" />
        <text x="60" y="100" textAnchor="middle" fill="white" fontSize="11" fontWeight="bold">Event</text>
        <text x="60" y="115" textAnchor="middle" fill="white" fontSize="11" fontWeight="bold">Source</text>
      </g>

      {/* Broker */}
      <g>
        <rect x="200" y="80" width="80" height="50" rx="6" fill="#ec4899" opacity="0.9" />
        <text x="240" y="110" textAnchor="middle" fill="white" fontSize="11" fontWeight="bold">Broker</text>
      </g>

      {/* Processor */}
      <g>
        <rect x="380" y="80" width="100" height="50" rx="6" fill="#3b82f6" opacity="0.9" />
        <text x="430" y="110" textAnchor="middle" fill="white" fontSize="11" fontWeight="bold">Processor</text>
      </g>

      {/* Arrows */}
      <defs>
        <marker id="time-arrow" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
          <polygon points="0 0, 8 3, 0 6" fill="#0E4714" />
        </marker>
      </defs>
      <line x1="100" y1="105" x2="195" y2="105" stroke="#0E4714" strokeWidth="2" markerEnd="url(#time-arrow)" />
      <line x1="280" y1="105" x2="375" y2="105" stroke="#0E4714" strokeWidth="2" markerEnd="url(#time-arrow)" />

      {/* Time labels */}
      <g>
        <text x="60" y="150" textAnchor="middle" fill="#0E4714" fontSize="12" fontWeight="bold">Event Time</text>
        <text x="60" y="165" textAnchor="middle" fill="#0E4714" fontSize="9">(when it happened)</text>
      </g>
      <g>
        <text x="240" y="150" textAnchor="middle" fill="#0E4714" fontSize="12" fontWeight="bold">Ingestion Time</text>
        <text x="240" y="165" textAnchor="middle" fill="#0E4714" fontSize="9">(when it entered)</text>
      </g>
      <g>
        <text x="430" y="150" textAnchor="middle" fill="#0E4714" fontSize="12" fontWeight="bold">Processing Time</text>
        <text x="430" y="165" textAnchor="middle" fill="#0E4714" fontSize="9">(when processed)</text>
      </g>

      {/* Timeline */}
      <line x1="20" y1="185" x2="480" y2="185" stroke="#0E4714" strokeWidth="1" />
      <text x="250" y="198" textAnchor="middle" fill="#0E4714" fontSize="10">Time →</text>
    </svg>
  )
}
