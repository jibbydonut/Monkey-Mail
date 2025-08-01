import os
from mailchimp_marketing import Client
from mailchimp_marketing.api_client import ApiClientError
from dotenv import load_dotenv
import logging
from helper import setup_logger
from mailchimp import (
    extract_monkeymail_data, 
    list_local_files, 
    list_missing_files, 
    list_s3_objects, 
    filter_json_keys, 
    upload_missing_to_s3,
    upload_temp_data_to_s3,
    extract_and_sync_monkeymail_data,
    temporary_directory
)
import time
from datetime import datetime
import json
import shutil

# Setup logging
setup_logger()
load_dotenv()

# Define Variables
monkey_api = os.getenv("MAILCHIMP_API_KEY")
start_range = "2025-05-01T00:00:00+00:00"  # ISO8601 Format
end_range = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")

# Optional permanent output directory (set to None if you only want temp processing)
permanent_output_dir = os.path.join(os.getcwd(), 'local_data/api_content')

endpoints = [
    {"name": "list", "client": "campaigns", "req_campaign": False},
    {"name": "get_email_activity_for_campaign", "client": "reports", "req_campaign": True}
]

aws_access = os.getenv('ACCESS_KEY')
aws_secret = os.getenv('SECRET_ACCESS_KEY')
bucket_name = os.getenv("AWS_BUCKET_NAME")
s3_prefix = 'monkey_mail_python/'

def main():
    """Main execution function with multiple approach options."""
    
    logging.info("Starting Mailchimp data extraction and sync process...")
    
    # OPTION 1: Complete workflow using the new integrated function
    # This handles everything: extract -> temp -> permanent (optional) -> S3 sync
    try:
        extract_and_sync_monkeymail_data(
            monkey_api=monkey_api,
            endpoints=endpoints,
            start_range=start_range,
            end_range=end_range,
            aws_access=aws_access,
            aws_secret=aws_secret,
            bucket_name=bucket_name,
            permanent_output_dir=permanent_output_dir  # Set to None if you don't want permanent storage
        )
        logging.info("Complete workflow finished successfully!")
        return
    
    except Exception as e:
        logging.error(f"Complete workflow failed: {e}")
        logging.info("Falling back to step-by-step approach...")
    
    # OPTION 2: Step-by-step approach with temporary directories
    try:
        step_by_step_approach()
    except Exception as e:
        logging.error(f"Step-by-step approach failed: {e}")
        raise

def step_by_step_approach():
    """Step-by-step approach using temporary directories."""
    
    # Step 1: Extract data to temporary directory
    logging.info("Step 1: Extracting Mailchimp data to temporary directory...")
    temp_dir = extract_monkeymail_data(
        monkey_api=monkey_api,
        endpoints=endpoints,
        start_range=start_range,
        end_range=end_range,
        output_dir=permanent_output_dir  # This will copy to permanent location too
    )
    
    try:
        # Step 2: List what we have locally (from temp or permanent)
        if permanent_output_dir and os.path.exists(permanent_output_dir):
            logging.info("Step 2: Using permanent directory for S3 comparison...")
            local_files = list_local_files(permanent_output_dir)
            local_dir_for_upload = permanent_output_dir
        else:
            logging.info("Step 2: Using temporary directory for S3 comparison...")
            local_files = list_local_files(temp_dir)
            local_dir_for_upload = temp_dir
        
        logging.info(f"Found {len(local_files)} local files")
        
        # Step 3: Check what's already in S3
        logging.info("Step 3: Checking existing files in S3...")
        s3_files = list_s3_objects(aws_access, aws_secret, bucket_name, s3_prefix.rstrip('/'))
        s3_json_files = filter_json_keys(s3_files)
        logging.info(f"Found {len(s3_json_files)} JSON files in S3")
        
        # Step 4: Find missing files
        logging.info("Step 4: Identifying missing files...")
        missing_files = list_missing_files(local_files, s3_json_files)
        
        # Step 5: Upload missing files
        if missing_files:
            logging.info(f"Step 5: Uploading {len(missing_files)} missing files to S3...")
            upload_missing_to_s3(
                aws_access=aws_access,
                aws_secret=aws_secret,
                missing_files=missing_files,
                local_dir=local_dir_for_upload,
                bucket_name=bucket_name
            )
        else:
            logging.info("Step 5: No missing files found - all files are already in S3")
        
        logging.info("Step-by-step process completed successfully!")
        
    finally:
        # Step 6: Clean up temporary directory
        if temp_dir and os.path.exists(temp_dir):
            logging.info(f"Step 6: Cleaning up temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir, ignore_errors=True)

def temp_only_approach():
    """Alternative approach: work entirely with temporary directories."""
    
    logging.info("Starting temp-only approach...")
    
    with temporary_directory() as temp_dir:
        # Extract data to temporary directory only
        logging.info("Extracting data to temporary directory...")
        extract_monkeymail_data(
            monkey_api=monkey_api,
            endpoints=endpoints,
            start_range=start_range,
            end_range=end_range,
            output_dir=None  # No permanent storage
        )
        
        # Upload everything directly from temp to S3
        logging.info("Uploading all data directly from temp directory to S3...")
        upload_temp_data_to_s3(
            aws_access=aws_access,
            aws_secret=aws_secret,
            temp_dir=temp_dir,
            bucket_name=bucket_name,
            s3_prefix=s3_prefix
        )
        
        logging.info("Temp-only approach completed successfully!")
    # Temporary directory is automatically cleaned up here

def legacy_approach():
    """Legacy approach for comparison - only works if permanent directory exists."""
    
    if not os.path.exists(permanent_output_dir):
        logging.error(f"Legacy approach requires existing directory: {permanent_output_dir}")
        return
    
    logging.info("Using legacy approach with existing permanent directory...")
    
    # Your original code logic
    s3_list = list_s3_objects(aws_access, aws_secret, bucket_name, s3_prefix.rstrip('/'))
    cleaned_s3_list = filter_json_keys(s3_list)
    local_list = list_local_files(permanent_output_dir)
    missing_files = list_missing_files(local_list, cleaned_s3_list)
    
    if missing_files:
        upload_missing_to_s3(aws_access, aws_secret, missing_files, permanent_output_dir, bucket_name)
    else:
        logging.info("No missing files found using legacy approach")

if __name__ == "__main__":
    
    # Validate required environment variables
    required_vars = ['MAILCHIMP_API_KEY', 'ACCESS_KEY', 'SECRET_ACCESS_KEY', 'AWS_BUCKET_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logging.error(f"Missing required environment variables: {missing_vars}")
        exit(1)
    
    # Run the main process
    try:
        main()
    except Exception as e:
        logging.error(f"Main process failed: {e}")
        exit(1)
    
    logging.info("All operations completed successfully!")

# Uncomment below to test different approaches:

# Alternative usage examples:
# temp_only_approach()      # Work entirely with temp directories
# legacy_approach()         # Use your original approach (requires existing permanent dir)