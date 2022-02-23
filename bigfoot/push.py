import typer

from bigfoot.constants import Target

push_app = typer.Typer()


@push_app.command()
def push(target: Target, dry_run: bool = True):
    if dry_run:
        typer.echo(f"Generating manifest for pushing to {target}")
    else:
        typer.echo(f"Pushing to {target}")
