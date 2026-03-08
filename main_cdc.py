"""Main entry point for CDC Producer API Service"""
import uvicorn


def main():
    """Run the CDC Producer API server on port 8001"""
    uvicorn.run(
        "src.api.cdc_server:app",
        host="0.0.0.0",
        port=8001,
        reload=False,
        log_level="info"
    )


if __name__ == "__main__":
    main()
