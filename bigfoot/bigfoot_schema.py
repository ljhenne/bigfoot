import json
from typing import List

from google.cloud.bigquery.schema import SchemaField


class BigfootSchema:
    api_repr: List[SchemaField]
    name: str

    def __init__(self, name: str):
        self.name = name
        local_filepath = f"../data/gradient__{self.name}.json"
        with open(local_filepath, "r") as file:
            self.api_repr = [SchemaField.from_api_repr(field) for field in json.load(file)]
