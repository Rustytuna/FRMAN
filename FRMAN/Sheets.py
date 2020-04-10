#!/usr/bin/env python3

# Author::    Justin Flannery  (mailto:juftin@juftin.com)

"""GOOGLE SHEETS OPERATIONS FOR READING/WRITING DATA."""

from datetime import datetime
from os.path import exists
from pickle import dump as pkdump
from pickle import load as pkload

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pandas import DataFrame, to_datetime, to_timedelta
from pygsheets import authorize
from logging import getLogger
from adjuftments.Utils import LOG_LEVEL
from adjuftments.Utils import yml
yaml = yml()

getLogger('googleapiclient.discovery').setLevel(LOG_LEVEL.ERROR)


class Sheets:
    """MAIN CLASS TO INTERACT WITH GOOGLE SHEETS DATA."""

    def __init__(self, token, client_secret):
        """GOOGLE API AUTORIZATION."""
        creds = None
        if exists(token):
            with open(token, "rb") as gtoken:
                creds = pkload(gtoken)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    client_secret, yaml["google_sheets"]["scope"])
                creds = flow.run_local_server()
            with open(token, "wb") as gtoken:
                pkdump(creds, gtoken)
        self.credentials = creds
        service = build("sheets", "v4", credentials=creds,
                        cache_discovery=False)
        self.service = service

    def get_sheet(self, sheet_id, sheet_range):
        """GET SHEET RAW DATA."""
        sheet = self.service.spreadsheets(). \
            values().get(spreadsheetId=sheet_id,
                         range=sheet_range,
                         valueRenderOption="UNFORMATTED_VALUE",
                         dateTimeRenderOption="FORMATTED_STRING").execute()
        values = sheet["values"]
        return values

    @staticmethod
    def prepare_sheet(sheet, dtype=None):
        """FORMAT DATA INTO A DATAFRAME."""
        dataframe = DataFrame(sheet, columns=sheet[0], dtype=dtype)
        del sheet
        dataframe.drop(0, axis=0, inplace=True)
        dataframe.reset_index(drop=True, inplace=True)
        return dataframe

    def sheet_to_df(self, sheet_id, sheet_name=None, sheet_range=None,
                    value_render="UNFORMATTED_VALUE"):
        """CONVERT GOOGLE SHEET TO DATAFRAME."""
        pyg = authorize(custom_credentials=self.credentials)
        spreadsheet = pyg.open_by_key(sheet_id)
        if sheet_name is None:
            wks = spreadsheet[0]
        else:
            wks = spreadsheet.worksheet_by_title(sheet_name)
        if sheet_range is not None:
            start = sheet_range.split(":")[0]
            end = sheet_range.split(":")[1]
        else:
            start = None
            end = None
        dataframe = wks.get_as_df(
            start=start, end=end, value_render=value_render)
        return dataframe

    def backup_to_sheet(self, sheet_id, dataframe, sheet_name=None,
                        set_range=(2, 1)):
        """APPEND DATAFRAME INTO GOOGLE SHEET."""
        pyg = authorize(custom_credentials=self.credentials)
        spreadsheet = pyg.open_by_key(sheet_id)
        if sheet_name is None:
            wks = spreadsheet[0]
        else:
            wks = spreadsheet.worksheet_by_title(sheet_name)
        wks.clear(start="A1", end="ZZ10000")
        dataframe["Date"] = dataframe["Date"].apply(
            lambda x: x.strftime("%m/%d/%Y"))
        dataframe["Timestamp"] = datetime.now()
        wks.set_dataframe(dataframe, set_range,
                          copy_index=False, copy_head=True, nan="")
        return dataframe

    def append(self, values, sheet_id, start='A1', end=None,
               dimension='ROWS', overwrite=False, sheet_name=None):
        """APPEND VLAUE TO SHEET."""
        pyg = authorize(custom_credentials=self.credentials)
        spreadsheet = pyg.open_by_key(sheet_id)
        if sheet_name is None:
            wks = spreadsheet[0]
        else:
            wks = spreadsheet.worksheet_by_title(sheet_name)
        wks.append_table(values=values, start=start, end=end,
                         dimension=dimension, overwrite=overwrite)

    @staticmethod
    def convert_excel_time(excel_time):
        """UNSERIALIZE EXCEL DATETIME."""
        return to_datetime("1899-12-30") + to_timedelta(excel_time, "D")
