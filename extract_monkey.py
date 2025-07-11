# Extract Data from Monkey Mail Campaigns

import os
from mailchimp_marketing import Client
from mailchimp_marketing.api_client import ApiClientError
from dotenv import load_dotenv

load_dotenv()

monkey_api = os.getenv("MAILCHIMP_API_KEY")

since_create_time="2025-06-01T00:00:00+00:00" #ISO8601 Format

try:
    client = Client()
    client.set_config({"api_key":monkey_api})
    response = client.campaigns.list(since_create_time="")
    print(response)
except ApiClientError as error:
    print(error)