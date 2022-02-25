import os

import typer
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from bigfoot.google_spreadsheet import BatchUpdateRequestBody, CreateSheetResponse, GoogleSheetsSchema

# Created credentials using: https://developers.google.com/workspace/guides/create-credentials#desktop-app
CREDENTIALS = os.getenv("OAUTH2_CREDENTIALS_FILE")

# Find scopes here: https://developers.google.com/identity/protocols/oauth2/scopes#sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.metadata.readonly"]

sheets_app = typer.Typer()


def get_google_service(service_name: str):
    creds = None

    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    if service_name == "sheets":
        version = "v4"
    elif service_name == "drive":
        version = "v3"
    else:
        raise ValueError(f"Unsupported service: {service_name}")

    return build(service_name, version, credentials=creds)


def create_sheet(schema: GoogleSheetsSchema) -> CreateSheetResponse:
    spreadsheets = get_google_service("sheets").spreadsheets()
    google_spreadsheet = schema.mk_google_spreadsheet().dict(exclude_none=True)
    spreadsheet = spreadsheets.create(body=google_spreadsheet, fields="spreadsheetId,sheets").execute()
    return CreateSheetResponse.parse_obj(spreadsheet)


def format_sheet(schema: GoogleSheetsSchema, spreadsheet_id: str, sheet_id: str):
    dimension_groups = schema.mk_dimension_groups(sheet_id)
    batch_update_request_body = BatchUpdateRequestBody.add_dimension_groups_request(dimension_groups).dict()
    spreadsheets = get_google_service("sheets").spreadsheets()
    request = spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_request_body)
    return request.execute()


@sheets_app.command()
def create(name: str):
    schema = GoogleSheetsSchema(name=name)
    create_sheet_response = create_sheet(schema)
    print(format_sheet(schema, create_sheet_response.spreadsheet_id, create_sheet_response.sheets[0].properties.sheet_id))
