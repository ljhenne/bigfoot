import typer

from bigfoot.constants import Target

pull_app = typer.Typer()


@pull_app.command()
def pull(target: Target):
    if target == Target.BIGQUERY:

        typer.echo(f"Pulling from {target}")
