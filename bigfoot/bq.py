from copy import deepcopy
from pathlib import Path
from typing import List, Optional, Union

import typer

from google.cloud import bigquery

bq_app = typer.Typer()
_bq_client: Optional[bigquery.Client] = None


def recursive_count_rows(schema: List[bigquery.SchemaField]) -> int:
    count = 0
    for schema_field in schema:
        if schema_field.fields:
            count += recursive_count_rows(schema_field.fields)
        else:
            count += 1
    return count


def save_backup_schema(client: bigquery.Client, schema: List[bigquery.SchemaField], dataset: str, table_name: str):
    original_schema = deepcopy(schema)
    filepath = Path(f"../data/{dataset}__{table_name}__backup.json")
    if filepath.exists():
        filepath.unlink()
    client.schema_to_json(schema, filepath)
    return original_schema


@bq_app.command()
def get(gcp_project: str, dataset: str, table_name: str):
    table_id = f"{gcp_project}.{dataset}.{table_name}"
    typer.echo(f"Pulling schema from {table_id}")
    client = bigquery.Client()
    table = client.get_table(table_id)
    client.schema_to_json(table.schema, f"../data/{dataset}__{table_name}.json")


@bq_app.command()
def create_snapshot(gcp_project: str, dataset: str, table_name: str):
    global _bq_client
    if not _bq_client:
        _bq_client = bigquery.Client()
    query = """
        DECLARE current_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
        DECLARE expiration_ts TIMESTAMP;
        DECLARE dataset, original_table_id, original_table_name, project_id, query, snapshot_table_id STRING;
        SET dataset = @dataset;
        SET expiration_ts = TIMESTAMP_ADD(current_ts, INTERVAL 7 DAY);
        SET project_id = @gcp_project;
        SET original_table_name = @table_name;
        SET original_table_id = (SELECT CONCAT('`', project_id, '`.', dataset, '.', original_table_name));
        SET snapshot_table_id = (
            SELECT CONCAT('`', project_id, '`.', dataset, '.', original_table_name, '_backup_', UNIX_MILLIS(current_ts))
        );
        SET query = (CONCAT(
          "CREATE SNAPSHOT TABLE ",
          snapshot_table_id,
          " CLONE ",
          original_table_id,
          " OPTIONS(expiration_timestamp = TIMESTAMP '",
          expiration_ts,
          "');"  
        ));
        EXECUTE IMMEDIATE query;
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dataset", "STRING", dataset),
            bigquery.ScalarQueryParameter("gcp_project", "STRING", gcp_project),
            bigquery.ScalarQueryParameter("table_name", "STRING", table_name)
        ]
    )
    query_job = _bq_client.query(query, job_config=job_config)
    query_job.result()
    if query_job.errors:
        raise Exception(f"Query has errors: {query_job.errors}")


@bq_app.command()
def create_table(gcp_project: str, dataset: str, table_name: str):
    client = bigquery.Client()
    schema = client.schema_from_json(f"../data/{dataset}__{table_name}.json")
    table_id: Union[bigquery.table.TableReference, str] = f"{gcp_project}.{dataset}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)  # noqa; PyCharm type hinting doesn't pick up doc strings
    client.create_table(table)


@bq_app.command()
def update_schema(gcp_project: str, dataset: str, table_name: str):
    global _bq_client
    if not _bq_client:
        _bq_client = bigquery.Client()
    table_id = f"{gcp_project}.{dataset}.{table_name}"
    table = _bq_client.get_table(table_id)
    create_snapshot(gcp_project, dataset, table_name)
    original_schema = save_backup_schema(_bq_client, table.schema, dataset, table_name)
    new_schema = _bq_client.schema_from_json(f"../data/{dataset}__{table_name}.json")
    table.schema = new_schema
    table = _bq_client.update_table(table, ["schema"])

    if recursive_count_rows(table.schema) == recursive_count_rows(original_schema) + 1:
        print("A new column has been added.")
    else:
        print("The column has not been added.")
