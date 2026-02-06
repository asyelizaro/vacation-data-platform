import requests
import pandas as pd
import boto3
from io import StringIO

countries = ['AM', 'CU', 'EG', 'GE', 'IT', 'JP', 'KR', 'KZ', 'RU', 'TR', 'VN']
year = 2025

all_data = []

for country in countries:
    url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/{country}"
    response = requests.get(url)

    if response.status_code == 200:
        for item in response.json():
            item["country"] = country
            all_data.append(item)

df = pd.DataFrame(all_data)
print(df.head())


MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "vacation-data-platform"
FILE_NAME = "holidays"

s3 = boto3.client(
    "s3",
    endpoint_url = MINIO_ENDPOINT,
    aws_access_key_id = ACCESS_KEY,
    aws_secret_access_key = SECRET_KEY,
    region_name = "us-east-1"
)

csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)


s3.put_object(
    Bucket = BUCKET_NAME,
    Key = f"raw/holidays/{FILE_NAME}.csv",
    Body = csv_buffer.getvalue()
)

