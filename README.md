# Mailchimp Data Extraction Tool

A Python tool for extracting data from Mailchimp campaigns and managing the results in AWS S3 storage.

## Overview

This tool provides functionality to:
- Extract data from Mailchimp campaigns using the Marketing API
- Handle both campaign-specific and general endpoint calls
- Store extracted data as JSON files locally
- Sync data to AWS S3 with duplicate detection
- Manage file uploads efficiently

## Prerequisites

- Python 3.7+
- Mailchimp Marketing API key
- AWS credentials (for S3 upload functionality)
- Required Python packages (see Installation)

## Installation

1. Clone or download this repository
2. Install required dependencies:

```bash
pip install mailchimp-marketing python-dotenv boto3
```

3. Create a `.env` file in the project root with your credentials:

```env
MAILCHIMP_API_KEY=your_mailchimp_api_key_here
ACCESS_KEY=your_aws_access_key
SECRET_ACCESS_KEY=your_aws_secret_key
AWS_BUCKET_NAME=your_s3_bucket_name
```

## Usage

### Basic Data Extraction

```python
from your_module import extract_monkeymail_data

# Define endpoints to extract data from
endpoints = [
    {
        "client": "campaigns",
        "name": "list",
        "req_campaign": False
    },
    {
        "client": "reports",
        "name": "get",
        "req_campaign": True
    }
]

# Extract data for a date range
extract_monkeymail_data(
    monkey_api="your_api_key",
    endpoints=endpoints,
    start_range="2024-01-01T00:00:00Z",
    end_range="2024-12-31T23:59:59Z",
    output_dir="./output"
)
```

### S3 Management

```python
from your_module import list_s3_objects, upload_missing_to_s3

# List existing S3 objects
s3_keys = list_s3_objects(
    aws_access="your_access_key",
    aws_secret="your_secret_key",
    bucket_name="your_bucket",
    prefix="monkey_mail_python"
)

# Upload missing files
upload_missing_to_s3(
    aws_access="your_access_key",
    aws_secret="your_secret_key",
    missing_files=["file1.json", "file2.json"],
    local_dir="./output",
    bucket_name="your_bucket"
)
```

## Configuration

### Endpoint Configuration

Each endpoint in the `endpoints` list should have:

- `client`: The Mailchimp API client name (e.g., "campaigns", "reports")
- `name`: The method name to call on the client
- `req_campaign`: Boolean indicating if the endpoint requires a campaign ID

### Date Range Format

Use ISO 8601 format for date ranges:
- `start_range`: "YYYY-MM-DDTHH:MM:SSZ"
- `end_range`: "YYYY-MM-DDTHH:MM:SSZ"

## Output Structure

The tool creates the following directory structure:

```
output_dir/
├── campaigns_list/
│   └── 20240101_120000_campaigns_list.json
├── reports_get/
│   └── 20240101_120000_reports_get.json
└── ...
```

Each JSON file contains:
- Timestamp in filename for tracking
- Complete API response data
- UTF-8 encoded content

## Features

### Error Handling
- Automatic retry with exponential backoff (up to 3 attempts)
- Comprehensive logging for debugging
- Graceful handling of API client errors

### Data Management
- Automatic directory creation
- Duplicate file detection
- Efficient S3 synchronization
- JSON file filtering and validation

### Campaign Processing
- Automatic campaign discovery within date ranges
- Batch processing for campaign-specific endpoints
- Individual campaign data extraction

## API Reference

### Main Functions

#### `extract_monkeymail_data(monkey_api, endpoints, start_range, end_range, output_dir)`
Extracts data from specified Mailchimp endpoints.

**Parameters:**
- `monkey_api`: Mailchimp API key
- `endpoints`: List of endpoint configurations
- `start_range`: Start date for data extraction
- `end_range`: End date for data extraction
- `output_dir`: Local directory for output files

#### `list_s3_objects(aws_access, aws_secret, bucket_name, prefix)`
Lists objects in S3 bucket with specified prefix.

#### `upload_missing_to_s3(aws_access, aws_secret, missing_files, local_dir, bucket_name)`
Uploads missing files to S3 storage.

### Utility Functions

- `filter_json_keys(s3_keys)`: Filters S3 keys to JSON files only
- `list_local_files(local_dir)`: Lists all local files recursively
- `list_missing_files(local_files, s3_files)`: Identifies files missing in S3

## Logging

The tool uses Python's logging module with a custom logger setup. Logs include:
- API connectivity status
- Campaign discovery results
- File write operations
- Error messages and retry attempts
- Upload progress and completion

## Dependencies

- `mailchimp-marketing`: Official Mailchimp Marketing API client
- `python-dotenv`: Environment variable management
- `boto3`: AWS SDK for S3 operations
- `logging`: Built-in Python logging
- `json`: JSON data handling
- `os`: Operating system interface
- `time`: Time-related functions
- `datetime`: Date and time manipulation

## Error Handling

The tool includes robust error handling for:
- API connection failures
- Invalid credentials
- Network timeouts
- File system errors
- S3 upload failures

## Best Practices

1. **Rate Limiting**: The tool includes built-in retry logic to handle API rate limits
2. **Incremental Sync**: Use date ranges to avoid re-downloading existing data
3. **Monitoring**: Check logs regularly for any processing issues
4. **Security**: Keep API keys and AWS credentials secure in environment variables

## Troubleshooting

### Common Issues

1. **API Connection Failed**
   - Verify your Mailchimp API key is correct
   - Check internet connectivity
   - Ensure API key has necessary permissions

2. **S3 Upload Errors**
   - Verify AWS credentials
   - Check S3 bucket permissions
   - Ensure bucket exists and is accessible

3. **File Not Found Errors**
   - Verify output directory exists
   - Check file permissions
   - Ensure sufficient disk space

## License

This project is provided as-is for educational and development purposes.

## Contributing

Feel free to submit issues and enhancement requests!