"""Main entry point for Livestream Assistant Agent"""
import uvicorn


def main():
    """
    Run the Feature & Sentiment API server on port 8000.
    
    Note: CDC Producer endpoints now run separately via main_cdc.py on port 8001.
    To start the CDC service, run: python main_cdc.py
    """
    uvicorn.run(
        "src.api.server:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )


if __name__ == "__main__":
    main()
