import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Livestream Assistant Agent - Presentation',
  description: 'Real-time AI-powered insights for livestream engagement',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className="bg-slide-bg min-h-screen">
        {children}
      </body>
    </html>
  )
}
