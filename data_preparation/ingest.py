"""
Data Ingestion Script for Amazon Metadata 2023
Downloads data from HuggingFace and uploads to S3 with minimal disk usage.

Usage:
    python ingest.py --mode s3                          # Download and upload to S3 (default)
    python ingest.py --mode local                       # Download to local only
    python ingest.py --mode s3 --file-type reviews      # Process review files instead of metadata
    python ingest.py --mode s3 --skip-delete            # Keep local files after upload
    python ingest.py --mode s3 --files meta_A.jsonl ... # Process only selected files
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import List, Optional
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from huggingface_hub import hf_hub_download
from dotenv import load_dotenv
from data_preparation.meta_files import ALL_META_FILES, ALL_REVIEWS_FILES

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

# Dataset configuration
HUGGINGFACE_REPO = "McAuley-Lab/Amazon-Reviews-2023"
HUGGINGFACE_REPO_TYPE = "dataset"
HUGGINGFACE_BASE_PATH = {
    "meta": "raw/meta_categories",
    "reviews": "raw/reviews"
}

class DataIngestionConfig:
    """Configuration for data ingestion"""
    
    def __init__(self):
        self.aws_bucket_name = os.getenv("AWS_BUCKET_NAME")
        self.aws_profile_name = os.getenv("AWS_PROFILE_NAME", "default")
        self.aws_region = os.getenv("AWS_REGION", "ap-northeast-1")
        self.local_data_dir = os.getenv("LOCAL_DATA_DIR", "amazon_metadata_2023/raw")
        self.s3_prefix = os.getenv("S3_PREFIX", "amazon_metadata_2023/raw")
        
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
    
    def download_file(self, meta_file: str, file_type: str = "meta") -> Optional[str]:
        """
        Download a single file from HuggingFace
        
        Args:
            meta_file: Name of the metadata file to download
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            logger.info(f"📥 Downloading {meta_file}...")
            file_path = hf_hub_download(
                repo_id=HUGGINGFACE_REPO,
                repo_type=HUGGINGFACE_REPO_TYPE,
                filename=f"{HUGGINGFACE_BASE_PATH[file_type.lower()]}/{meta_file}",
                local_dir=f"{self.config.local_data_dir}/{file_type.lower()}",
            )
            
            # Get file size
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
            logger.info(f"✅ Downloaded {meta_file} ({file_size:.2f} MB)")
            return file_path
            
        except Exception as e:
            logger.error(f"❌ Failed to download {meta_file}: {e}")
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
    
    def process_file(self, meta_file: str, delete_after_upload: bool = True, file_type: str = "meta") -> bool:
        """
        Process a single file: download, upload to S3, and optionally delete
        
        Args:
            meta_file: Name of the metadata file
            delete_after_upload: Whether to delete local file after S3 upload
            file_type: Type of file to process (meta or reviews)
        Returns:
            True if successful, False otherwise
        """
        # Download
        file_path = self.download_file(meta_file, file_type)
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
        
        for idx, meta_file in enumerate(files, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing file {idx}/{stats['total']}: {meta_file}")
            logger.info(f"{'='*60}")
            
            success = self.process_file(meta_file, delete_after_upload, file_type)
            
            if success:
                stats["success"] += 1
            else:
                stats["failed"] += 1
                stats["failed_files"].append(meta_file)
        
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
        description="Ingest Amazon Metadata 2023 from HuggingFace to S3/Local",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download and upload to S3 (default)
  python ingest.py --mode s3
  
  # Download to local only
  python ingest.py --mode local
  
  # Process review files instead of metadata
  python ingest.py --mode s3 --file-type reviews
  
  # Keep local files after upload
  python ingest.py --mode s3 --skip-delete
  
  # Process specific files
  python ingest.py --mode s3 --files meta_Electronics.jsonl meta_Books.jsonl
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
        help="Specific files to process (space-separated)"
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
    
    # Determine which files to process
    if args.files:
        files_to_process = args.files
        logger.info(f"Processing {len(files_to_process)} specified files")
    else:
        files_to_process = ALL_META_FILES if args.file_type == "meta" else ALL_REVIEWS_FILES
        logger.info(f"Processing all {len(files_to_process)} categories")
    
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

