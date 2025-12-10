"""
Data Ingestion Script for Amazon Metadata 2023
Downloads public Google Drive files and optionally uploads to S3.

Usage:
    python ingest.py --mode s3 --files <gdrive_url ...>       # Download and upload to S3 (default)
    python ingest.py --mode local --files <gdrive_url ...>    # Download to local only
    python ingest.py --mode s3 --file-type reviews --files ...# Tag files as reviews for S3 prefixing
    python ingest.py --mode s3 --skip-delete --files ...      # Keep local files after upload
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import List, Optional
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from data_preparation.download_file import download_google_drive_file

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('ingest.log')
    ]
)
logger = logging.getLogger(__name__)

class DataIngestionConfig:
    """Configuration for data ingestion"""
    
    def __init__(self):
        self.aws_bucket_name = os.getenv("AWS_BUCKET_NAME")
        self.aws_profile_name = os.getenv("AWS_PROFILE_NAME", "default")
        self.aws_region = os.getenv("AWS_REGION", "ap-northeast-1")
        self.local_data_dir = os.getenv("LOCAL_DATA_DIR", "amazon_product_data_2014/raw")
        self.s3_prefix = os.getenv("S3_PREFIX", "amazon_product_data_2014/raw")
        
    def validate(self, mode: str) -> bool:
        """Validate configuration based on mode"""
        if mode == "s3":
            if not self.aws_bucket_name:
                logger.error("AWS_BUCKET_NAME not set in environment variables")
                return False
        return True


class DataIngestor:
    """Handles data ingestion from HuggingFace to local/S3"""
    
    def __init__(self, config: DataIngestionConfig, mode: str = "s3"):
        self.config = config
        self.mode = mode
        self.s3_client = None
        
        if mode == "s3":
            self._initialize_s3_client()
    
    def _initialize_s3_client(self):
        """Initialize S3 client with proper error handling"""
        try:
            session = boto3.Session(
                profile_name=self.config.aws_profile_name,
                region_name=self.config.aws_region
            )
            self.s3_client = session.client("s3")
            
            # Test connection by checking if bucket exists
            self.s3_client.head_bucket(Bucket=self.config.aws_bucket_name)
            logger.info(f"✅ Connected to S3 bucket: {self.config.aws_bucket_name}")
            
        except NoCredentialsError:
            logger.error("❌ AWS credentials not found. Please configure AWS credentials.")
            raise
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"❌ S3 bucket '{self.config.aws_bucket_name}' does not exist")
            elif error_code == '403':
                logger.error(f"❌ Access denied to S3 bucket '{self.config.aws_bucket_name}'")
            else:
                logger.error(f"❌ Error connecting to S3: {e}")
            raise
    
    def download_file(self, file_url: str, file_type: str = "meta") -> Optional[str]:
        """
        Download a single file from Google Drive
        
        Args:
            file_url: Public Google Drive URL to download
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            output_dir = f"{self.config.local_data_dir}/{file_type.lower()}"
            logger.info(f"📥 Downloading from {file_url} to {output_dir} ...")
            file_path = download_google_drive_file(
                url=file_url,
                output_dir=output_dir,
            )
            
            # Get file size
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
            logger.info(f"✅ Downloaded {Path(file_path).name} ({file_size:.2f} MB)")
            return file_path
            
        except Exception as e:
            logger.error(f"❌ Failed to download from {file_url}: {e}")
            return None
    
    def upload_to_s3(self, file_path: str, file_type: str = "meta") -> bool:
        """
        Upload file to S3
        
        Args:
            file_path: Local path to file
            
        Returns:
            True if successful, False otherwise
        """
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return False
        
        try:
            file_name = os.path.basename(file_path)
            s3_key = f"{self.config.s3_prefix}/{file_type.lower()}/{file_name}"
            
            logger.info(f"📤 Uploading to s3://{self.config.aws_bucket_name}/{s3_key}...")
            
            # Upload with progress callback
            self.s3_client.upload_file(
                file_path,
                self.config.aws_bucket_name,
                s3_key
            )
            
            logger.info(f"✅ Uploaded {file_type.upper()} {file_name} to S3")
            return True
            
        except ClientError as e:
            logger.error(f"❌ Failed to upload {file_type.upper()} {file_name} to S3: {e}")
            return False
    
    def delete_local_file(self, file_path: str, file_type: str = "meta") -> bool:
        """
        Delete local file to save disk space
        
        Args:
            file_path: Path to file to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"🗑️  Deleted local {file_type.upper()} {os.path.basename(file_path)} file: {file_path}")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Failed to delete {file_type.upper()} {os.path.basename(file_path)} file: {file_path}: {e}")
            return False
    
    def process_file(self, file_url: str, delete_after_upload: bool = True, file_type: str = "meta") -> bool:
        """
        Process a single file: download, upload to S3, and optionally delete
        
        Args:
            file_url: Public Google Drive URL to download
            delete_after_upload: Whether to delete local file after S3 upload
            file_type: Type of file to process (meta or reviews)
        Returns:
            True if successful, False otherwise
        """
        # Download
        file_path = self.download_file(file_url, file_type)
        if not file_path:
            return False
        
        # Upload to S3 if in s3 mode
        if self.mode == "s3":
            upload_success = self.upload_to_s3(file_path, file_type)
            if not upload_success:
                return False
            
            # Delete local file if requested
            if delete_after_upload:
                self.delete_local_file(file_path, file_type)
        
        return True
    
    def ingest_all(
        self, 
        files: List[str], 
        delete_after_upload: bool = True,
        file_type: str = "meta"
    ) -> dict:
        """
        Ingest multiple files
        
        Args:
            files: List of file names to process
            delete_after_upload: Whether to delete local files after S3 upload
            file_type: Type of file to process (meta or reviews)
        Returns:
            Dictionary with success/failure statistics
        """
        stats = {
            "total": len(files),
            "success": 0,
            "failed": 0,
            "failed_files": []
        }
        
        logger.info(f"🚀 Starting ingestion of {stats['total']} files in '{self.mode}' mode")
        
        for idx, file_url in enumerate(files, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing file {idx}/{stats['total']}: {file_url}")
            logger.info(f"{'='*60}")
            
            success = self.process_file(file_url, delete_after_upload, file_type)
            
            if success:
                stats["success"] += 1
            else:
                stats["failed"] += 1
                stats["failed_files"].append(file_url)
        
        # Print summary
        logger.info(f"\n{'='*60}")
        logger.info("📊 INGESTION SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total {file_type.upper()} files: {stats['total']}")
        logger.info(f"✅ Successful: {stats['success']}")
        logger.info(f"❌ Failed: {stats['failed']}")
        
        if stats["failed_files"]:
            logger.warning(f"Failed {file_type.upper()} files: {', '.join(stats['failed_files'])}")
        
        return stats


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Ingest Amazon Product Data 2014 from Google Drive to S3/Local",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download and upload to S3 (default)
  python ingest.py --mode s3 --files https://drive.google.com/file/d/<ID>/view
  
  # Download to local only
  python ingest.py --mode local --files https://drive.google.com/file/d/<ID>/view
  
  # Process review files instead of metadata
  python ingest.py --mode s3 --file-type reviews --files https://drive.google.com/file/d/<ID>/view
  
  # Keep local files after upload
  python ingest.py --mode s3 --skip-delete --files https://drive.google.com/file/d/<ID>/view
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["s3", "local"],
        default="s3",
        help="Ingestion mode: 's3' uploads to S3, 'local' downloads only"
    )
    
    parser.add_argument(
        "--skip-delete",
        action="store_true",
        help="Keep local files after uploading to S3"
    )
    
    parser.add_argument(
        "--files",
        nargs="+",
        required=True,
        help="Public Google Drive file URLs to process (space-separated)"
    )

    parser.add_argument(
        "--file-type",
        choices=["meta", "reviews"],
        default="meta",
        help="Type of file to process (meta or reviews)"
    )
    
    args = parser.parse_args()
    
    # Initialize configuration
    config = DataIngestionConfig()
    
    # Validate configuration
    if not config.validate(args.mode):
        logger.error("Configuration validation failed. Please check your .env file.")
        sys.exit(1)
    
    # Determine which files to process (Google Drive URLs are required)
    files_to_process = args.files
    logger.info(f"Processing {len(files_to_process)} specified Google Drive URLs")
    
    # Initialize ingestor
    try:
        ingestor = DataIngestor(config, mode=args.mode)
    except Exception as e:
        logger.error(f"Failed to initialize ingestor: {e}")
        sys.exit(1)
    
    # Run ingestion
    delete_after_upload = not args.skip_delete
    stats = ingestor.ingest_all(files_to_process, delete_after_upload, file_type=args.file_type)
    
    # Exit with appropriate code
    if stats["failed"] > 0:
        sys.exit(1)
    else:
        logger.info("\n🎉 All files processed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    main()

