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
import boto3

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

                #create subfolder
                output_subdir = os.path.join(output_dir, name)
                os.makedirs(output_subdir, exist_ok=True)

                output_path = os.path.join(output_subdir,f"{runtime}_{name}.json")

                

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


aws_access = os.getenv('ACCESS_KEY')
aws_secret = os.getenv('SECRET_ACCESS_KEY')
bucket_name = os.getenv("AWS_BUCKET_NAME")
prefix = 'monkey_mail_python'


def list_s3_objects(aws_access,aws_secret,bucket_name,prefix):

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
    # Filter the list to only include .json files
    return [key for key in s3_keys if key.endswith('.json')]

filter_json_keys(list_s3_objects(aws_access,aws_secret,bucket_name))

def list_local_files(local_dir):

    # List local json files
    file_paths = []

    for root, _, files in os.walk(local_dir):
        for name in files:
            full_path = os.path.join(root, name)
            rel_path = os.path.relpath(full_path, local_dir).replace("\\","/")
            file_paths.append(rel_path)

    return file_paths

local_dir = "local_data/api_content"
print(list_local_files(local_dir))





