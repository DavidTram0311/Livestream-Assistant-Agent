import SlideContainer from '../SlideContainer'

export default function TitleSlide() {
  return (
    <SlideContainer className="items-center text-center">
      <h1 className="slide-title mb-8">Livestream Assistant Agent</h1>
      <p className="slide-subtitle mb-12">Dat Tram - SDC 2</p>
      <p className="slide-body text-slide-muted italic">
        Real-time AI-powered insights for livestream engagement
      </p>
    </SlideContainer>
  )
}
