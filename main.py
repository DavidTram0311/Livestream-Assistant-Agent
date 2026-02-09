"""Main entry point for Livestream Assistant Agent"""
import uvicorn


def main():
    """Run the CDC Producer API server"""
    uvicorn.run(
        "src.api.server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )


if __name__ == "__main__":
    main()
