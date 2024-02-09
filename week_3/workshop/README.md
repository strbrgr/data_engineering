# Data Extraction

## Data Extraction

### What is the best way to extract data from an API

There are two ways of doing this:

1. Having a paginated getter
2. Streaming the data line by line

Both approaches have their use cases depending on the nature of the data source and the requirements of the application.

The first approach is suitable for paginated APIs where data is retrieved in pages, while the second approach is suitable for streaming data directly from a source like a JSONL file.

#### 1. Paginated Getter Approach

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

#### 2. Streaming JSON Lines Approach

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

## Data Normalisation

### Why normalising?

Normalising data is often just metadata work (normalising columns, string to number).

### What is the issue with JSON?

- JSON is a data transfer format. It does not have a schema.
- No enforced types.
- Slow to aggregate. No way of selecting columns like we can with .parquet files.
- Not suited for analytical usage.

### How can `dlt` help with that?

`dlt` can help with flattening deeply nested JSON data. By using generators, `dlt` enables us to standardise things like correction of timestamps and converting of column names.

To do that, we:

- define the connection to load to: `pipeline = dlt.pipeline(destination='duckdb', dataset_name='taxi_rides')`
- run with merge disposition `info = pipeline.run(data,
					table_name="rides",
					write_disposition="merge",
          primary_key="record_hash")`

In this example `data` is our JSON file. Our JSON document got flattened and sub-documents got split into separate tables
