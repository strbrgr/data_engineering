import requests


def parquet_ddl():
    for month in range(1, 13):
        url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        filename = ""
        if month < 10:
            filename = f"green_tripdata_2022-0{month}.parquet"
        else:
            filename = f"green_tripdata_2022-{month}.parquet"
        response = requests.get(url + filename)
        if response.status_code == 200:
            with open(f"{filename}", "wb") as file:
                file.write(response.content)
            print("File downloaded successfully!")
        else:
            print("Failed to download file.")


parquet_ddl()
