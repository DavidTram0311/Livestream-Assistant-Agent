"""Main entry point for Livestream Assistant Agent"""
import uvicorn


def main():
    """Run the unified API server with CDC Producer, Feature Extraction, and Sentiment Analysis"""
    uvicorn.run(
        "src.api.server:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )


if __name__ == "__main__":
    main()
