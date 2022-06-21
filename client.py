import json
import os

from io import BytesIO
from pandas import read_csv

from google.cloud import storage
from google.oauth2 import service_account


class GooglePlay:
    def __init__(self, service_account_path: os.path, bucket_id: str):
        self._client = storage.Client(
            credentials=service_account.Credentials.from_service_account_file(
                filename=service_account_path
            )
        )
        self._bucket_id = bucket_id

    def get_stats(
            self,
            point: str,
            package_name: str,
            month: str,
            dimension: str = 'overview',
            bucked_id: str = None
    ):
        bucket_name = bucked_id or self._bucket_id
        bucket = self._client.get_bucket(bucket_or_name=bucket_name)

        blob_name = f'stats/{point}/{point}_{package_name}_{month}_{dimension}.csv'
        blob = bucket.get_blob(blob_name)

        try:
            raw_data = blob.download_as_bytes().decode('utf-16').encode('utf-8')
        except AttributeError:
            return []

        csv_data = read_csv(BytesIO(raw_data), sep=',')
        json_data = csv_data.to_json(orient='table', date_format='iso', index=False)
        result = json.loads(json_data)

        schema = result['schema']['fields']
        metrics = [field['name'].lower().replace(' ', '_') for field in schema]

        data = result['data']
        payload = []

        for row in data:
            values = list(row.values())
            payload.append(dict(zip(metrics, values)))

        return payload
