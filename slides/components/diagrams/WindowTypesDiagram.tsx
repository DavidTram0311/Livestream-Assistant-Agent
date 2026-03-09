export default function WindowTypesDiagram() {
  return (
    <svg viewBox="0 0 700 280" className="w-full max-w-4xl mx-auto">
      {/* Tumbling Windows */}
      <g transform="translate(0, 0)">
        <text x="10" y="20" fill="#0E4714" fontSize="14" fontWeight="bold">Tumbling</text>
        <rect x="10" y="30" width="80" height="40" rx="4" fill="#3b82f6" opacity="0.8" />
        <rect x="95" y="30" width="80" height="40" rx="4" fill="#3b82f6" opacity="0.8" />
        <rect x="180" y="30" width="80" height="40" rx="4" fill="#3b82f6" opacity="0.8" />
        <text x="50" y="55" textAnchor="middle" fill="white" fontSize="11">W1</text>
        <text x="135" y="55" textAnchor="middle" fill="white" fontSize="11">W2</text>
        <text x="220" y="55" textAnchor="middle" fill="white" fontSize="11">W3</text>
        <line x1="10" y1="80" x2="260" y2="80" stroke="#0E4714" strokeWidth="1" />
        <text x="280" y="55" fill="#0E4714" fontSize="10">Fixed, non-overlapping</text>
      </g>

      {/* Sliding/Hopping Windows */}
      <g transform="translate(0, 70)">
        <text x="10" y="20" fill="#0E4714" fontSize="14" fontWeight="bold">Sliding/Hopping</text>
        <rect x="10" y="30" width="100" height="40" rx="4" fill="#9333ea" opacity="0.7" />
        <rect x="50" y="30" width="100" height="40" rx="4" fill="#9333ea" opacity="0.7" />
        <rect x="90" y="30" width="100" height="40" rx="4" fill="#9333ea" opacity="0.7" />
        <text x="60" y="55" textAnchor="middle" fill="white" fontSize="11">W1</text>
        <text x="100" y="55" textAnchor="middle" fill="white" fontSize="11">W2</text>
        <text x="140" y="55" textAnchor="middle" fill="white" fontSize="11">W3</text>
        <line x1="10" y1="80" x2="260" y2="80" stroke="#0E4714" strokeWidth="1" />
        <text x="280" y="55" fill="#0E4714" fontSize="10">Fixed, overlapping</text>
      </g>

      {/* Session Windows */}
      <g transform="translate(0, 140)">
        <text x="10" y="20" fill="#0E4714" fontSize="14" fontWeight="bold">Session</text>
        <rect x="10" y="30" width="60" height="40" rx="4" fill="#ec4899" opacity="0.8" />
        <rect x="120" y="30" width="90" height="40" rx="4" fill="#ec4899" opacity="0.8" />
        <rect x="260" y="30" width="40" height="40" rx="4" fill="#ec4899" opacity="0.8" />
        <text x="40" y="55" textAnchor="middle" fill="white" fontSize="11">S1</text>
        <text x="165" y="55" textAnchor="middle" fill="white" fontSize="11">S2</text>
        <text x="280" y="55" textAnchor="middle" fill="white" fontSize="11">S3</text>
        <line x1="10" y1="80" x2="320" y2="80" stroke="#0E4714" strokeWidth="1" />
        <text x="75" y="95" fill="#0E4714" fontSize="9">gap</text>
        <text x="220" y="95" fill="#0E4714" fontSize="9">gap</text>
        <text x="350" y="55" fill="#0E4714" fontSize="10">Activity-based, gap-triggered</text>
      </g>

      {/* Global Window */}
      <g transform="translate(0, 210)">
        <text x="10" y="20" fill="#0E4714" fontSize="14" fontWeight="bold">Global</text>
        <rect x="10" y="30" width="300" height="40" rx="4" fill="#f97316" opacity="0.8" />
        <text x="160" y="55" textAnchor="middle" fill="white" fontSize="11">Single Window (All Data)</text>
        <line x1="10" y1="80" x2="320" y2="80" stroke="#0E4714" strokeWidth="1" />
        <text x="350" y="55" fill="#0E4714" fontSize="10">Unbounded, single window</text>
      </g>

      {/* Time axis label */}
      <text x="160" y="275" textAnchor="middle" fill="#0E4714" fontSize="12">Time →</text>
    </svg>
  )
}
