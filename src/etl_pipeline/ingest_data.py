"""
Main CLI Entry Point for Amazon Product Data Ingestion

This script orchestrates the download of Amazon product data from Google Drive
and optionally uploads it to AWS S3. It uses the gdown library for reliable
Google Drive downloads and boto3 for S3 operations.

Usage:
    python -m main --mode s3 --files <gdrive_url> [<gdrive_url> ...]
    python -m main --mode local --files <gdrive_url> [<gdrive_url> ...]
    
Environment Variables (via .env file):
    AWS_BUCKET_NAME: S3 bucket name for uploads
    AWS_PROFILE_NAME: AWS profile to use (default: 'default')
    AWS_REGION: AWS region (default: 'ap-northeast-1')
    LOCAL_DATA_DIR: Local directory for downloads (default: 'amazon_product_data_2014/raw')
    S3_PREFIX: S3 key prefix (default: 'amazon_product_data_2014/raw')
"""

import argparse
import sys

from ingest import DataIngestionConfig, DataIngestor, logger as ingest_logger


def parse_args(argv=None):
    """
    Parse command-line arguments for data ingestion.
    
    Args:
        argv: Optional list of arguments (defaults to sys.argv)
        
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        prog="main",
        description="Ingest Amazon Product Data 2014 from Google Drive to S3/Local",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download metadata and upload to S3 (default mode)
  python -m main --mode s3 --files https://drive.google.com/file/d/<ID>/view
  
  # Download multiple files to local only
  python -m main --mode local --files https://drive.google.com/file/d/<ID1>/view https://drive.google.com/file/d/<ID2>/view
  
  # Process review files instead of metadata
  python -m main --mode s3 --file-type reviews --files https://drive.google.com/file/d/<ID>/view
  
  # Keep local files after S3 upload (don't delete)
  python -m main --mode s3 --skip-delete --files https://drive.google.com/file/d/<ID>/view

Notes:
  - Ensure you have configured AWS credentials for S3 mode
  - Google Drive URLs should be public or accessible
  - Files are downloaded using gdown library for reliability
        """
    )

    parser.add_argument(
        "--mode",
        choices=["s3", "local"],
        default="local",
        help="Ingestion mode: 's3' downloads and uploads to S3, 'local' downloads only (default: local)"
    )

    parser.add_argument(
        "--skip-delete",
        action="store_true",
        help="Keep local files after uploading to S3 (only applies in s3 mode)"
    )

    parser.add_argument(
        "--files",
        nargs="+",
        required=True,
        metavar="URL",
        help="One or more public Google Drive file URLs to download (space-separated)"
    )

    parser.add_argument(
        "--file-type",
        choices=["meta", "reviews"],
        default="meta",
        help="Type of file to process: 'meta' for metadata or 'reviews' for review data (default: meta)"
    )

    return parser.parse_args(argv)


def main(argv=None):
    """
    Main entry point for data ingestion.
    
    Args:
        argv: Optional list of command-line arguments
        
    Returns:
        Exit code: 0 for success, 1 for failure
    """
    # Parse command-line arguments
    args = parse_args(argv)
    
    ingest_logger.info("="*60)
    ingest_logger.info("🚀 Amazon Product Data Ingestion Tool")
    ingest_logger.info("="*60)

    # Initialize and validate configuration
    config = DataIngestionConfig()
    if not config.validate(args.mode):
        ingest_logger.error("❌ Configuration validation failed. Please check your .env file.")
        ingest_logger.error("   Required for S3 mode: AWS_BUCKET_NAME")
        return 1

    # Log configuration
    ingest_logger.info(f"📋 Configuration:")
    ingest_logger.info(f"   Mode: {args.mode}")
    ingest_logger.info(f"   File type: {args.file_type}")
    ingest_logger.info(f"   Number of files: {len(args.files)}")
    if args.mode == "s3":
        ingest_logger.info(f"   S3 Bucket: {config.aws_bucket_name}")
        ingest_logger.info(f"   Delete after upload: {not args.skip_delete}")
    ingest_logger.info(f"   Local directory: {config.local_data_dir}")

    # Initialize ingestor
    try:
        ingestor = DataIngestor(config, mode=args.mode)
    except Exception as e:
        ingest_logger.error(f"❌ Failed to initialize ingestor: {e}")
        ingest_logger.error("   Please check your AWS credentials and configuration.")
        return 1

    # Run ingestion
    delete_after_upload = not args.skip_delete
    stats = ingestor.ingest_all(
        files=args.files,
        delete_after_upload=delete_after_upload,
        file_type=args.file_type
    )

    # Determine exit code based on results
    if stats["failed"] > 0:
        ingest_logger.error(f"❌ Ingestion completed with {stats['failed']} failure(s)")
        return 1

    ingest_logger.info("🎉 All files processed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
