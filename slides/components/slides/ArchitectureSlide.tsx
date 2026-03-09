import SlideContainer from '../SlideContainer'
import ArchitectureDiagram from '../diagrams/ArchitectureDiagram'

export default function ArchitectureSlide() {
  return (
    <SlideContainer>
      <h1 className="slide-title text-center mb-8">Architecture Overview</h1>
      <ArchitectureDiagram />
    </SlideContainer>
  )
}
