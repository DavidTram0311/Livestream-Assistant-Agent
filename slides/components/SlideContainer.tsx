import React from 'react'

interface SlideContainerProps {
  children: React.ReactNode
  className?: string
}

export default function SlideContainer({ children, className = '' }: SlideContainerProps) {
  return (
    <div className={`min-h-screen w-full bg-slide-bg flex flex-col justify-center px-8 md:px-16 lg:px-24 py-12 ${className}`}>
      {children}
    </div>
  )
}
