import os
from mailchimp_marketing import Client
from mailchimp_marketing.api_client import ApiClientError
from dotenv import load_dotenv
import logging
from helper import setup_logger
from mailchimp import extract_monkeymail_data
import time
from datetime import datetime
import json

monkey_api = os.getenv("MAILCHIMP_API_KEY")

start_range="2025-05-01T00:00:00+00:00" #ISO8601 Format
end_range = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")

output_dir = os.path.join(os.getcwd(),'local_data/api_content')

endpoints = [
    {"name": "list", "client": "campaigns", "req_campaign": False},
    {"name": "get_email_activity_for_campaign", "client": "reports", "req_campaign": True}
]

extract_monkeymail_data(monkey_api, endpoints, start_range, end_range, output_dir)