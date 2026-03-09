import SlideContainer from '../SlideContainer'
import WindowTypesDiagram from '../diagrams/WindowTypesDiagram'
import TimeNotionsDiagram from '../diagrams/TimeNotionsDiagram'

export default function AggregationConceptsSlide() {
  return (
    <SlideContainer>
      <h1 className="slide-title mb-6">Real-Time Aggregation - Concepts</h1>
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="concept-card">
          <h2 className="slide-heading mb-4">Window Types</h2>
          <WindowTypesDiagram />
        </div>

        <div className="space-y-4">
          <div className="concept-card">
            <h2 className="slide-heading mb-4">Time Notions</h2>
            <TimeNotionsDiagram />
          </div>

          <div className="concept-card">
            <h2 className="slide-heading mb-2">Watermarks</h2>
            <ul className="bullet-list text-base">
              <li><span>Mechanism to track event-time progress</span></li>
              <li><span>Determines when windows can close</span></li>
              <li><span>Handles late-arriving data via grace periods</span></li>
            </ul>
          </div>
        </div>
      </div>
    </SlideContainer>
  )
}
