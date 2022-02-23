import typer

from bigfoot.pull import pull_app
from bigfoot.push import push_app

app = typer.Typer()
app.add_typer(pull_app, name="pull")
app.add_typer(push_app, name="push")


if __name__ == "__main__":
    app()
