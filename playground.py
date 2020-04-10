#!/usr/bin/env python3

# Author::    Justin Flannery  (mailto:juftin@juftin.com)

"""
Testing File for FRMAN Airtable / Google Sheets Integration
"""

from logging import getLogger
from pprint import pprint
from pandas import DataFrame
from yaml import dump

from pandas import read_pickle, concat

from FRMAN.Airtable import Airtable
from FRMAN.Sheets import Sheets
from FRMAN.Utils import yml, pretty_print
from FRMAN.SQL import SQL

yaml = yml()
log = getLogger(yaml["logger"])
sql = SQL(yaml["sql"]["db_file"])


# airtableRequests = Airtable(base=yaml["airtable"]["base"],
#                             api_key=yaml["airtable"]["api_key"],
#                             table="Requests")
# requests_df = airtableRequests.get_airtable_raw(params=None)
# pretty_print(requests_df)

# SheetsConn = Sheets(token=yaml["google_sheets"]["token"],
#                     client_secret=yaml["google_sheets"]["client_secret"])
# sheets_df = SheetsConn.sheet_to_df(sheet_id=yaml["google_sheets"]["sheet_id"],
#                                    sheet_name="Requests")
# sql.ingest_df(dataframe=sheets_df, table_name="sheets", if_exists="replace")
# sheets_df.to_pickle("sheets.pickle")


# sheets_df = read_pickle("sheets.pickle")
# columns = list(sheets_df.columns)
# column_dict = [{key: ""} for key in columns]
# with open("migration/sheets.yaml", 'w') as file:
#     documents = dump(data=column_dict, stream=file, sort_keys=False)

# for diction in column_dict["english_to_spanish"]:
#     print(diction)
#     for key, value in diction.items():
#         print(key, value)
#         flip_column_dict.append({value: key})
#
# with open("migration/sheets2.yaml", 'w') as file:
#     documents = dump(data=flip_column_dict, stream=file, sort_keys=False)


def break_out_language_requests(request_df: DataFrame) -> tuple:
    """
    Break out the English vs. Spanish Requests
    """
    english_columns = [value for key, value in
                       sheet_yaml["spanish_to_english"].items()]
    spanish_columns = [key for key, value in
                       sheet_yaml["spanish_to_english"].items()]
    language_question = "Quiere responder en ingles o espanol? | Do you want " \
                        "to respond in English or Spanish?"
    english_df = request_df[
        request_df[
            language_question] == "I prefer to respond in English."].copy()
    spanish_df = request_df[
        request_df[
            language_question] == "Prefiero responder en espanol."].copy()

    english_df.drop
    english_df.drop(spanish_columns + [language_question], inplace=True, axis=1)
    spanish_df.drop(english_columns + [language_question], inplace=True, axis=1)
    spanish_df.rename(columns=sheet_yaml["spanish_to_english"], inplace=True)
    english_df["language"] = "english"
    spanish_df["language"] = "spanish"
    return english_df, spanish_df


sheet_yaml = yml("migration/sheets.yaml")
request_df = read_pickle("sheets.pickle")
english_requests, spanish_requests = break_out_language_requests(
    request_df=request_df)

master_needs = []
needs = english_requests["What do you need?"].unique().tolist()
default_needs = sheet_yaml["default_needs"]
need_match = dict()
for unique_needs in needs:
    original_unique_needs = unique_needs
    needs_list = list()
    for default_need in default_needs:
        if default_need in unique_needs:
            needs_list.append(default_need)
            unique_needs = unique_needs.replace(default_need + ", ", "")
            unique_needs = unique_needs.replace(default_need, "")
    if len(unique_needs) > 0:
        needs_list.append("Other")
        addl_info = f"What do you need?:\n{unique_needs}\n\n"
    else:
        addl_info = ""
    need_match[original_unique_needs] = {"needs": needs_list,
                                         "other": addl_info}

english_requests["needs_list"] = english_requests["What do you need?"].apply(
    lambda x: need_match[x]["needs"])
english_requests["additional_info"] = english_requests[
    "What do you need?"].apply(
    lambda x: need_match[x]["other"])
info_columns = ["additional_info",
                "Add any additional detail about what you need here"]
english_requests["info"] = english_requests[info_columns].apply(
    lambda row: "\n".join(row.values.astype(str)).strip(), axis=1)
english_requests.fillna("", inplace=True)
english_requests.drop(info_columns + ["What do you need?"], inplace=True,
                      axis=1)
pretty_print(english_requests.tail(200))
print(english_requests[english_requests["needs_list"] == []])
