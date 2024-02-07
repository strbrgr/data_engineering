# Data Extraction

## What is the best way to extract data from an API

There are two ways of doing this:

1. Having a paginated getter
2. Streaming the data line by line

Both approaches have their use cases depending on the nature of the data source and the requirements of the application.

The first approach is suitable for paginated APIs where data is retrieved in pages, while the second approach is suitable for streaming data directly from a source like a JSONL file.

### 1. Paginated Getter Approach

```python
import requests

BASE_API_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

def paginated_getter():
    page_number = 1
    while True:
        params = {'page': page_number}
        response = requests.get(BASE_API_URL, params=params)
        response.raise_for_status()
        page_json = response.json()
        if page_json:
            yield page_json
            page_number += 1
        else:
            break

if __name__ == '__main__':
    for page_data in paginated_getter():
        print(page_data)

```

### 2. Streaming JSON Lines Approach

```python
import requests
import json

url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"

def stream_download_jsonl(url):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    for line in response.iter_lines():
        if line:
            yield json.loads(line)

# Usage example
row_counter = 0
for row in stream_download_jsonl(url):
    print(row)
    row_counter += 1
    if row_counter >= 5:
        break

```
