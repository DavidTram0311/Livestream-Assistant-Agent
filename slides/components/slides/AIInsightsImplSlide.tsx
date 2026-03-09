import SlideContainer from '../SlideContainer'

export default function AIInsightsImplSlide() {
  return (
    <SlideContainer>
      <h1 className="slide-title mb-6">AI Insights - Implementation</h1>
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="concept-card">
          <h2 className="slide-heading mb-4">This Project&apos;s Flow</h2>
          <div className="slide-code text-xs">
            <pre>{`LLM Insight Service (Python)
    └── Consumes: streaming.combined_stats
            │
            ▼
OpenAI GPT-4o
    └── Input: Window stats 
              (gender/sentiment distribution)
    └── Output: Summary + Recommendations
            │
            ▼
Kafka Topic: streaming.llm_insights`}</pre>
          </div>
        </div>

        <div className="space-y-4">
          <div className="concept-card">
            <h2 className="slide-heading mb-4">Prompt Structure</h2>
            <ul className="bullet-list text-lg">
              <li><span><strong>System:</strong> &quot;You are a livestream analytics assistant...&quot;</span></li>
              <li><span><strong>User:</strong> Window stats with gender/sentiment %</span></li>
              <li><span><strong>Output:</strong> 1-3 sentence summary + recommendations</span></li>
            </ul>
          </div>

          <div className="concept-card">
            <h2 className="slide-heading mb-4">Tech Stack</h2>
            <ul className="bullet-list text-lg">
              <li><span><strong>OpenAI Python SDK</strong> (async)</span></li>
              <li><span><strong>GPT-4o</strong> model</span></li>
              <li><span>Structured output parsing</span></li>
              <li><span>Retry logic with exponential backoff</span></li>
            </ul>
          </div>
        </div>

        <div className="concept-card lg:col-span-2">
          <h2 className="slide-heading mb-4">Example Output</h2>
          <div className="bg-white/70 rounded-lg p-4 space-y-3">
            <div>
              <span className="font-bold text-slide-accent">Summary:</span>
              <span className="slide-body ml-2">&quot;Sentiment is mostly positive (72%). Audience is 60% male.&quot;</span>
            </div>
            <div>
              <span className="font-bold text-slide-accent">Recommendations:</span>
              <span className="slide-body ml-2">&quot;Consider engaging female viewers with targeted content.&quot;</span>
            </div>
          </div>
        </div>
      </div>
    </SlideContainer>
  )
}
