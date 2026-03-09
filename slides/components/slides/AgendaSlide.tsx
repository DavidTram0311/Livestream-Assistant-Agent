import SlideContainer from '../SlideContainer'

export default function AgendaSlide() {
  const agendaItems = [
    'Architecture Overview',
    'Data Ingestion',
    'Change Data Capture',
    'Stream Enrichment',
    'Real-Time Aggregation',
    'AI Insights',
    'Q&A',
  ]

  return (
    <SlideContainer>
      <h1 className="slide-title mb-12">Agenda</h1>
      <ul className="numbered-list max-w-2xl">
        {agendaItems.map((item, index) => (
          <li key={index}>
            <span>{item}</span>
          </li>
        ))}
      </ul>
    </SlideContainer>
  )
}
