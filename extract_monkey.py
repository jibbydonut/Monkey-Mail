# Extract Data from Monkey Mail Campaigns

import os
from mailchimp_marketing import Client
from mailchimp_marketing.api_client import ApiClientError
from dotenv import load_dotenv
import logging
from helper import setup_logger
import time
from datetime import datetime
import json

setup_logger()
load_dotenv()

monkey_api = os.getenv("MAILCHIMP_API_KEY")

start_range="2025-06-01T00:00:00+00:00" #ISO8601 Format
end_range = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")

output_dir = os.path.join(os.getcwd(),'local_data/api_content')

os.makedirs(output_dir, exist_ok=True)
runtime = datetime.now().strftime("%Y%m%d_%H%M%S")
filepath = os.path.join(output_dir,runtime+"_campaigns_list.json")

for i in range(3):
    try:
        client = Client()
        client.set_config({"api_key":monkey_api})
        response = client.ping.get()
        logging.info(response)
        data = client.campaigns.list(since_create_time=start_range, before_create_time=end_range)

        # check for valid data
        if data and 'campaigns' in data and isinstance(data['campaigns'],list): # check if data present, contains campaign and is a list
            if data['campaigns']:
                logging.info(f"Retrieved {len(data['campaigns'])} campaigns")
                with open(filepath,'wb') as file:
                    file.write(json.dumps(data,indent=2).encode('utf-8'))
                logging.info(f"Succesfully written to {filepath}")
            else:
                logging.warning("No campaigns found in date range")
        else:
            logging.error("Unexpected response format or empty result")
        
        break

    except ApiClientError as error:
        logging.error(f"Attempt {i+1} failed: {error}")
        time.sleep(2**i)


