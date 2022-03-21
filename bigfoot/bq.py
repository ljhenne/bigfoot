import typer
from typing import Union

from google.cloud import bigquery

bq_app = typer.Typer()


@bq_app.command()
def get(gcp_project: str, dataset: str, table_name: str):
    table_id = f"{gcp_project}.{dataset}.{table_name}"
    typer.echo(f"Pulling schema from {table_id}")
    client = bigquery.Client()
    table = client.get_table(table_id)
    client.schema_to_json(table.schema, f"../data/{dataset}__{table_name}.json")


@bq_app.command()
def create_table(gcp_project: str, dataset: str, table_name: str):
    client = bigquery.Client()
    schema = client.schema_from_json(f"../data/{dataset}__{table_name}.json")
    table_id: Union[bigquery.table.TableReference, str] = f"{gcp_project}.{dataset}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)  # noqa; PyCharm type hinting doesn't pick up doc strings
    client.create_table(table)
