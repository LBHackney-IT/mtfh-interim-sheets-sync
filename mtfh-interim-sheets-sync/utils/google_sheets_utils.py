"""
This module exposes functions that allows devs to interact with Google Sheets.
"""
from typing import Dict
import os
import json
from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SERVICE_ACCOUNT = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")


def read_google_sheets(spreadsheet_id: str, range_name: str) -> [Dict]:
    """
    Function that connects to a google sheets document and read the specified tab name and range.
    The first line is considerate as the data header.

    :param spreadsheet_id: The ID of the spreadsheets to read.
    :param range_name: The tab name and range to extract data from.
    :return: List of dicts of the data read from the google sheets document.
    """
    service_account_info = json.loads(SERVICE_ACCOUNT)
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)

    service_client = build('sheets', 'v4', credentials=credentials)

    # Call the Sheets API
    sheet_api = service_client.spreadsheets()
    result = sheet_api.values().get(spreadsheetId=spreadsheet_id,
                                    range=range_name).execute()
    values = result.get('values', [])

    reformatted_values = []
    for row in values[1:]:
        reformatted_row = {}
        for index, value in enumerate(row):
            reformatted_row[values[0][index]] = value
        reformatted_values.append(reformatted_row)
    return reformatted_values
