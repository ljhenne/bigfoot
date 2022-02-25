import typer

from google.cloud import bigquery

bq_app = typer.Typer()


@bq_app.command()
def get(gcp_project: str, dataset: str, table_name: str):
    table_id = f"{gcp_project}.{dataset}.{table_name}"
    typer.echo(f"Pulling schema from {table_id}")
    client = bigquery.Client()
    table = client.get_table(table_id)
    client.schema_to_json(table.schema, f"../data/{dataset}__{table_name}.json")
