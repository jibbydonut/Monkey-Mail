# Extract Data from Monkey Mail Campaigns
# https://mailchimp.com/developer/marketing/api/root/
# https://github.com/mailchimp/mailchimp-marketing-python?tab=readme-ov-file

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

def extract_monkeymail_data(monkey_api, endpoints, start_range, end_range, output_dir):

    os.makedirs(output_dir, exist_ok=True)
    runtime = datetime.now().strftime("%Y%m%d_%H%M%S")

    client = Client()
    client.set_config({"api_key":monkey_api})

    results = {}

    for i in range(3):
        try:
            #test connectivity
            response = client.ping.get()
            logging.info(response)

            # Go through each endpoint call, testing for campaign id requirement
            for endpoint in endpoints:
                name = endpoint["client"] + "_" + endpoint["name"]
                requires_campaign = endpoint.get("req_campaign",False)
                output_path = os.path.join(output_dir, f"{runtime}_{name}.json")

                # dynamic method calls
                target_client = getattr(client, endpoint["client"])
                target_method = getattr(target_client, endpoint["name"])

                # if req campaign then list campaigns in date range
                if requires_campaign:
                    campaigns = client.campaigns.list(since_create_time=start_range, before_create_time=end_range).get("campaigns")

                    logging.info(f"Found {len(campaigns)} campaigns for {name}")

                    endpoint_results = []
                    for campaign in campaigns:
                        campaign_id = campaign["id"]
                        data = target_method(campaign_id)
                        endpoint_results.append(data)
                    
                    with open(output_path,'wb') as file:
                        file.write(json.dumps(endpoint_results,indent=2).encode("utf-8"))
                    logging.info(f"Written to {output_path}")
                
                else:
                    data = target_method(since_create_time=start_range, before_create_time=end_range)

                    with open(output_path, 'wb') as file:
                        file.write(json.dumps(data, indent=2).encode('utf-8'))
                    logging.info(f"Written direct data to {output_path}")
            break

        except ApiClientError as error:
            logging.error(f"Attempt {i+1} failed: {error}")
            time.sleep(2**i)

    return results


# monkey_api = os.getenv("MAILCHIMP_API_KEY")

# start_range="2025-05-01T00:00:00+00:00" #ISO8601 Format
# end_range = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")

# output_dir = os.path.join(os.getcwd(),'local_data/api_content')

# endpoints = [
#     #{"name": "list", "client": "campaigns", "req_campaign": False},
#     {"name": "get_email_activity_for_campaign", "client": "reports", "req_campaign": True}
# ]

# extract_monkeymail_data(monkey_api, endpoints, start_range, end_range, output_dir)
