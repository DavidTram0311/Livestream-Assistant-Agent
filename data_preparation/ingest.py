"""
Data Ingestion Script for Amazon Metadata 2023
Downloads data from HuggingFace and uploads to S3 with minimal disk usage.

Usage:
    python ingest.py --mode s3                    # Download and upload to S3 (default)
    python ingest.py --mode local                 # Download to local only
    python ingest.py --mode s3 --categories 5     # Process only first 5 categories
    python ingest.py --mode s3 --skip-delete      # Keep local files after upload
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
HUGGINGFACE_BASE_PATH = "raw/meta_categories"

# All available metadata categories
ALL_META_FILES = [
    "meta_All_Beauty.jsonl",
    "meta_Amazon_Fashion.jsonl",
    "meta_Appliances.jsonl",
    "meta_Arts_Crafts_and_Sewing.jsonl",
    "meta_Automotive.jsonl",
    "meta_Baby_Products.jsonl",
    "meta_Beauty_and_Personal_Care.jsonl",
    "meta_Books.jsonl",
    "meta_CDs_and_Vinyl.jsonl",
    "meta_Cell_Phones_and_Accessories.jsonl",
    "meta_Clothing_Shoes_and_Jewelry.jsonl",
    "meta_Digital_Music.jsonl",
    "meta_Electronics.jsonl",
    "meta_Gift_Cards.jsonl",
    "meta_Grocery_and_Gourmet_Food.jsonl",
    "meta_Handmade_Products.jsonl",
    "meta_Health_and_Household.jsonl",
    "meta_Health_and_Personal_Care.jsonl",
    "meta_Home_and_Kitchen.jsonl",
    "meta_Industrial_and_Scientific.jsonl",
    "meta_Kindle_Store.jsonl",
    "meta_Magazine_Subscriptions.jsonl",
    "meta_Movies_and_TV.jsonl",
    "meta_Musical_Instruments.jsonl",
    "meta_Office_Products.jsonl",
    "meta_Patio_Lawn_and_Garden.jsonl",
    "meta_Pet_Supplies.jsonl",
    "meta_Software.jsonl",
    "meta_Sports_and_Outdoors.jsonl",
    "meta_Subscription_Boxes.jsonl",
    "meta_Tools_and_Home_Improvement.jsonl",
    "meta_Toys_and_Games.jsonl",
    "meta_Video_Games.jsonl"
]


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
    
    def download_file(self, meta_file: str) -> Optional[str]:
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
                filename=f"{HUGGINGFACE_BASE_PATH}/{meta_file}",
                local_dir=self.config.local_data_dir,
            )
            
            # Get file size
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
            logger.info(f"✅ Downloaded {meta_file} ({file_size:.2f} MB)")
            return file_path
            
        except Exception as e:
            logger.error(f"❌ Failed to download {meta_file}: {e}")
            return None
    
    def upload_to_s3(self, file_path: str) -> bool:
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
            s3_key = f"{self.config.s3_prefix}/{file_name}"
            
            logger.info(f"📤 Uploading to s3://{self.config.aws_bucket_name}/{s3_key}...")
            
            # Upload with progress callback
            self.s3_client.upload_file(
                file_path,
                self.config.aws_bucket_name,
                s3_key
            )
            
            logger.info(f"✅ Uploaded {file_name} to S3")
            return True
            
        except ClientError as e:
            logger.error(f"❌ Failed to upload {file_path} to S3: {e}")
            return False
    
    def delete_local_file(self, file_path: str) -> bool:
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
                logger.info(f"🗑️  Deleted local file: {file_path}")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Failed to delete {file_path}: {e}")
            return False
    
    def process_file(self, meta_file: str, delete_after_upload: bool = True) -> bool:
        """
        Process a single file: download, upload to S3, and optionally delete
        
        Args:
            meta_file: Name of the metadata file
            delete_after_upload: Whether to delete local file after S3 upload
            
        Returns:
            True if successful, False otherwise
        """
        # Download
        file_path = self.download_file(meta_file)
        if not file_path:
            return False
        
        # Upload to S3 if in s3 mode
        if self.mode == "s3":
            upload_success = self.upload_to_s3(file_path)
            if not upload_success:
                return False
            
            # Delete local file if requested
            if delete_after_upload:
                self.delete_local_file(file_path)
        
        return True
    
    def ingest_all(
        self, 
        files: List[str], 
        delete_after_upload: bool = True
    ) -> dict:
        """
        Ingest multiple files
        
        Args:
            files: List of file names to process
            delete_after_upload: Whether to delete local files after S3 upload
            
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
            
            success = self.process_file(meta_file, delete_after_upload)
            
            if success:
                stats["success"] += 1
            else:
                stats["failed"] += 1
                stats["failed_files"].append(meta_file)
        
        # Print summary
        logger.info(f"\n{'='*60}")
        logger.info("📊 INGESTION SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total files: {stats['total']}")
        logger.info(f"✅ Successful: {stats['success']}")
        logger.info(f"❌ Failed: {stats['failed']}")
        
        if stats["failed_files"]:
            logger.warning(f"Failed files: {', '.join(stats['failed_files'])}")
        
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
  
  # Process only first 5 categories (for testing)
  python ingest.py --mode s3 --categories 5
  
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
        files_to_process = ALL_META_FILES
        logger.info(f"Processing all {len(files_to_process)} categories")
    
    # Initialize ingestor
    try:
        ingestor = DataIngestor(config, mode=args.mode)
    except Exception as e:
        logger.error(f"Failed to initialize ingestor: {e}")
        sys.exit(1)
    
    # Run ingestion
    delete_after_upload = not args.skip_delete
    stats = ingestor.ingest_all(files_to_process, delete_after_upload)
    
    # Exit with appropriate code
    if stats["failed"] > 0:
        sys.exit(1)
    else:
        logger.info("\n🎉 All files processed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    main()

