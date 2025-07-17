import os
from mailchimp_marketing import Client
from mailchimp_marketing.api_client import ApiClientError
from dotenv import load_dotenv
import logging
from helper import setup_logger
from mailchimp import extract_monkeymail_data,list_local_files,list_missing_files,list_s3_objects,filter_json_keys,upload_missing_to_s3
import time
from datetime import datetime
import json

# Define Variables

monkey_api = os.getenv("MAILCHIMP_API_KEY")

start_range="2025-05-01T00:00:00+00:00" #ISO8601 Format
end_range = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")

output_dir = os.path.join(os.getcwd(),'local_data/api_content')

endpoints = [
    {"name": "list", "client": "campaigns", "req_campaign": False},
    {"name": "get_email_activity_for_campaign", "client": "reports", "req_campaign": True}
]
aws_access = os.getenv('ACCESS_KEY')
aws_secret = os.getenv('SECRET_ACCESS_KEY')
bucket_name = os.getenv("AWS_BUCKET_NAME")
prefix = 'monkey_mail_python'

local_dir = "local_data/api_content"

# extract_monkeymail_data(monkey_api, endpoints, start_range, end_range, output_dir)

s3_list = list_s3_objects(aws_access,aws_secret,bucket_name,prefix)
cleaned_s3_list = filter_json_keys(s3_list)
local_list = list_local_files(local_dir)
missing_files = list_missing_files(local_list,cleaned_s3_list)

upload_missing_to_s3(aws_access,aws_secret,missing_files,local_dir,bucket_name)