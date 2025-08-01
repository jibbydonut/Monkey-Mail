# Extract Data from Monkey Mail Campaigns
# https://mailchimp.com/developer/marketing/api/root/
# https://github.com/mailchimp/mailchimp-marketing-python?tab=readme-ov-file

import os
import tempfile
import shutil
from mailchimp_marketing import Client
from mailchimp_marketing.api_client import ApiClientError
from dotenv import load_dotenv
import logging
from helper import setup_logger
import time
from datetime import datetime
import json
import boto3
from contextlib import contextmanager

setup_logger()
load_dotenv()

@contextmanager
def temporary_directory():
    """Context manager for creating and cleaning up temporary directories."""
    temp_dir = tempfile.mkdtemp()
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

def extract_monkeymail_data(monkey_api, endpoints, start_range, end_range, output_dir=None):
    """
    Extract Mailchimp data to temporary directory and optionally copy to output_dir.
    
    Args:
        monkey_api: Mailchimp API key
        endpoints: List of endpoint configurations
        start_range: Start date range
        end_range: End date range
        output_dir: Optional permanent output directory (if None, only temp is used)
    
    Returns:
        str: Path to temporary directory containing the extracted data
    """
    runtime = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create temporary directory for all operations
    temp_dir = tempfile.mkdtemp(prefix=f"monkeymail_{runtime}_")
    
    try:
        client = Client()
        client.set_config({"api_key": monkey_api})

        results = {}

        for i in range(3):
            try:
                # Test connectivity
                response = client.ping.get()
                logging.info(response)

                # Go through each endpoint call, testing for campaign id requirement
                for endpoint in endpoints:
                    name = endpoint["client"] + "_" + endpoint["name"]
                    requires_campaign = endpoint.get("req_campaign", False)

                    # Create subfolder in temp directory
                    temp_subdir = os.path.join(temp_dir, name)
                    os.makedirs(temp_subdir, exist_ok=True)

                    temp_file_path = os.path.join(temp_subdir, f"{runtime}_{name}.json")

                    # Dynamic method calls
                    target_client = getattr(client, endpoint["client"])
                    target_method = getattr(target_client, endpoint["name"])

                    # If req campaign then list campaigns in date range
                    if requires_campaign:
                        campaigns = client.campaigns.list(
                            since_create_time=start_range, 
                            before_create_time=end_range
                        ).get("campaigns")

                        logging.info(f"Found {len(campaigns)} campaigns for {name}")

                        endpoint_results = []
                        for campaign in campaigns:
                            campaign_id = campaign["id"]
                            data = target_method(campaign_id)
                            endpoint_results.append(data)
                        
                        with open(temp_file_path, 'w', encoding='utf-8') as file:
                            json.dump(endpoint_results, file, indent=2, ensure_ascii=False)
                        logging.info(f"Written to temporary file: {temp_file_path}")
                    
                    else:
                        data = target_method(
                            since_create_time=start_range, 
                            before_create_time=end_range
                        )

                        with open(temp_file_path, 'w', encoding='utf-8') as file:
                            json.dump(data, file, indent=2, ensure_ascii=False)
                        logging.info(f"Written direct data to temporary file: {temp_file_path}")
                
                break

            except ApiClientError as error:
                logging.error(f"Attempt {i+1} failed: {error}")
                time.sleep(2**i)

        # If output_dir is specified, copy from temp to permanent location
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            for item in os.listdir(temp_dir):
                src_path = os.path.join(temp_dir, item)
                dst_path = os.path.join(output_dir, item)
                if os.path.isdir(src_path):
                    shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
                else:
                    shutil.copy2(src_path, dst_path)
            logging.info(f"Data copied to permanent location: {output_dir}")

        return temp_dir

    except Exception as e:
        # Clean up temp directory on error
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def list_s3_objects(aws_access, aws_secret, bucket_name, prefix):
    """List objects in S3 bucket with given prefix."""
    # Set s3 client
    s3_client = boto3.client(
        service_name='s3',
        aws_access_key_id=aws_access,
        aws_secret_access_key=aws_secret
    )

    paginator = s3_client.get_paginator('list_objects_v2')
    keys = []

    # Iterate through files
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys.append(obj['Key'])
    
    return keys

def filter_json_keys(s3_keys):
    """Filter the list to only include .json files."""
    return [key for key in s3_keys if key.endswith('.json')]

def list_local_files(local_dir):
    """List local json files with relative paths."""
    file_paths = []

    # Walk through all files/subdir in local_dir
    for root, _, files in os.walk(local_dir):
        for name in files:
            full_path = os.path.join(root, name)
            rel_path = os.path.relpath(full_path, local_dir).replace("\\", "/")
            file_paths.append(rel_path)

    return file_paths

def list_missing_files(local_files, s3_files):
    """Compare local files with S3 files and return missing ones."""
    # Get just filename (drop subdir)
    s3_filenames = {os.path.basename(path) for path in s3_files}

    # Get missing files in S3
    missing_local_paths = [
        path for path in local_files
        if os.path.basename(path) not in s3_filenames
    ]
    logging.info(f"Missing Files: {missing_local_paths}")
    return missing_local_paths

def upload_missing_to_s3(aws_access, aws_secret, missing_files, local_dir, bucket_name):
    """Upload missing files from local directory to S3."""
    # Set s3 client
    s3_client = boto3.client(
        service_name='s3',
        aws_access_key_id=aws_access,
        aws_secret_access_key=aws_secret
    )
    count = 0
    
    # Pass missing list and upload files
    for file in missing_files:
        missing_path = os.path.join(local_dir, file)
        s3_subdir = "monkey_mail_python/" + file
        s3_client.upload_file(missing_path, bucket_name, s3_subdir)
        count += 1
    
    logging.info(f"Uploaded {count} files")

def upload_temp_data_to_s3(aws_access, aws_secret, temp_dir, bucket_name, s3_prefix="monkey_mail_python/"):
    """
    Upload all data from temporary directory directly to S3.
    
    Args:
        aws_access: AWS access key
        aws_secret: AWS secret key
        temp_dir: Temporary directory containing the data
        bucket_name: S3 bucket name
        s3_prefix: S3 prefix for uploaded files
    """
    s3_client = boto3.client(
        service_name='s3',
        aws_access_key_id=aws_access,
        aws_secret_access_key=aws_secret
    )
    
    count = 0
    local_files = list_local_files(temp_dir)
    
    for file_path in local_files:
        local_full_path = os.path.join(temp_dir, file_path)
        s3_key = s3_prefix + file_path
        
        s3_client.upload_file(local_full_path, bucket_name, s3_key)
        count += 1
        logging.info(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    
    logging.info(f"Total uploaded: {count} files")
    return count

# Example usage function demonstrating the refactored approach
def extract_and_sync_monkeymail_data(monkey_api, endpoints, start_range, end_range, 
                                    aws_access, aws_secret, bucket_name, 
                                    permanent_output_dir=None):
    """
    Complete workflow: extract data to temp, optionally save to permanent dir, 
    and sync missing files to S3.
    """
    with temporary_directory() as temp_dir:
        try:
            # Extract data to temporary directory
            logging.info("Starting Mailchimp data extraction...")
            extract_monkeymail_data(monkey_api, endpoints, start_range, end_range, permanent_output_dir)
            
            # If we want to work with temp data, we can do so here
            temp_files = list_local_files(temp_dir)
            logging.info(f"Extracted {len(temp_files)} files to temporary directory")
            
            # Check what's already in S3
            s3_files = list_s3_objects(aws_access, aws_secret, bucket_name, "monkey_mail_python/")
            s3_json_files = filter_json_keys(s3_files)
            
            # Find missing files and upload them
            if permanent_output_dir and os.path.exists(permanent_output_dir):
                # Use permanent directory for comparison
                local_files = list_local_files(permanent_output_dir)
                missing_files = list_missing_files(local_files, s3_json_files)
                if missing_files:
                    upload_missing_to_s3(aws_access, aws_secret, missing_files, 
                                        permanent_output_dir, bucket_name)
            else:
                # Upload directly from temp directory
                upload_temp_data_to_s3(aws_access, aws_secret, temp_dir, bucket_name)
            
            logging.info("Mailchimp data extraction and sync completed successfully")
            
        except Exception as e:
            logging.error(f"Error in extract_and_sync_monkeymail_data: {e}")
            raise
        # Temporary directory is automatically cleaned up when exiting the context