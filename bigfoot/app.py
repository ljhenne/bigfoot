import typer

from bigfoot.bq import bq_app
from bigfoot.sheets import sheets_app

app = typer.Typer()
app.add_typer(bq_app, name="bq")
app.add_typer(sheets_app, name="sheets")


if __name__ == "__main__":
    app()
