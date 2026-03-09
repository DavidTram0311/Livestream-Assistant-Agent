import SlideContainer from '../SlideContainer'

export default function QnASlide() {
  return (
    <SlideContainer className="items-center text-center">
      <h1 className="slide-title mb-12">Questions?</h1>
      <div className="space-y-6">
        <p className="slide-body text-slide-muted">Thank you for your attention!</p>
        <div className="flex flex-col items-center gap-4 mt-8">
          <div className="flex items-center gap-3 slide-small">
          </div>
        </div>
      </div>
    </SlideContainer>
  )
}
