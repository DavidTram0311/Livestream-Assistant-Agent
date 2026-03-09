'use client'

import { useState, useEffect, useCallback } from 'react'
import TitleSlide from '@/components/slides/TitleSlide'
import AgendaSlide from '@/components/slides/AgendaSlide'
import ArchitectureSlide from '@/components/slides/ArchitectureSlide'
import DataIngestionConceptsSlide from '@/components/slides/DataIngestionConceptsSlide'
import DataIngestionImplSlide from '@/components/slides/DataIngestionImplSlide'
import CDCConceptsSlide from '@/components/slides/CDCConceptsSlide'
import CDCImplSlide from '@/components/slides/CDCImplSlide'
import EnrichmentConceptsSlide from '@/components/slides/EnrichmentConceptsSlide'
import EnrichmentImplSlide from '@/components/slides/EnrichmentImplSlide'
import AggregationConceptsSlide from '@/components/slides/AggregationConceptsSlide'
import AggregationImplSlide from '@/components/slides/AggregationImplSlide'
import AIInsightsImplSlide from '@/components/slides/AIInsightsImplSlide'
import QnASlide from '@/components/slides/QnASlide'

const slides = [
  TitleSlide,
  AgendaSlide,
  ArchitectureSlide,
  DataIngestionConceptsSlide,
  DataIngestionImplSlide,
  CDCConceptsSlide,
  CDCImplSlide,
  EnrichmentConceptsSlide,
  EnrichmentImplSlide,
  AggregationConceptsSlide,
  AggregationImplSlide,
  AIInsightsImplSlide,
  QnASlide,
]

export default function Home() {
  const [currentSlide, setCurrentSlide] = useState(0)

  const goToNextSlide = useCallback(() => {
    setCurrentSlide((prev) => Math.min(prev + 1, slides.length - 1))
  }, [])

  const goToPrevSlide = useCallback(() => {
    setCurrentSlide((prev) => Math.max(prev - 1, 0))
  }, [])

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'ArrowRight' || e.key === ' ') {
        e.preventDefault()
        goToNextSlide()
      } else if (e.key === 'ArrowLeft') {
        e.preventDefault()
        goToPrevSlide()
      }
    }

    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [goToNextSlide, goToPrevSlide])

  const CurrentSlideComponent = slides[currentSlide]

  return (
    <main className="relative">
      <CurrentSlideComponent />
      
      {/* Slide indicator */}
      <div className="slide-indicator">
        {currentSlide + 1} / {slides.length}
      </div>
    </main>
  )
}
