#!/usr/bin/env python3

# Author::    Justin Flannery  (mailto:juftin@juftin.com)

"""
Testing File for FRMAN Airtable / Google Sheets Integration
"""

from logging import getLogger
from pprint import pprint
from pandas import DataFrame
import names
import phonenumbers
import zipcodes
from yaml import dump
from requests.exceptions import HTTPError
from uuid import uuid4, UUID

from pandas import read_pickle, concat

from FRMAN.Airtable import Airtable
from FRMAN.Sheets import Sheets
from FRMAN.Utils import yml, pretty_print
from FRMAN.SQL import SQL

yaml = yml()
log = getLogger(yaml["logger"])
sql = SQL(yaml["sql"]["db_file"])


def get_airtable_df(table_name: str = "Requests") -> DataFrame:
    airtable_requests_object = Airtable(base=yaml["airtable"]["base"],
                                        api_key=yaml["airtable"]["api_key"],
                                        table=table_name)
    requests_df = airtable_requests_object.get_airtable_raw(params=None)
    return requests_df


def get_sheets_df():
    SheetsConn = Sheets(token=yaml["google_sheets"]["token"],
                        client_secret=yaml["google_sheets"]["client_secret"])
    sheets_df = SheetsConn.sheet_to_df(
        sheet_id=yaml["google_sheets"]["sheet_id"],
        sheet_name="Requests")
    sql.ingest_df(dataframe=sheets_df, table_name="sheets", if_exists="replace")
    sheets_df.to_pickle("sheets.pickle")


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
    english_df["language"] = "English"
    spanish_df["language"] = "Español"
    return english_df, spanish_df


def get_city_dict():
    cities = get_airtable_df("Cities")
    df_json = cities[["Zip Code", "id"]].set_index("id").to_dict(
        orient="index")
    new_dict = {value["Zip Code"]: key for key, value in df_json.items()}
    return new_dict


def apply_needs_info(english_requests: DataFrame):
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
            addl_info = "\n".join(["*****", "What do you need?:", unique_needs,
                                   "*****", "Additional Information:"])
        else:
            addl_info = ""
        need_match[original_unique_needs] = {"needs": needs_list,
                                             "other": addl_info}

    english_requests["needs_list"] = english_requests[
        "What do you need?"].apply(
        lambda x: need_match[x]["needs"])
    english_requests["additional_info"] = english_requests[
        "What do you need?"].apply(
        lambda x: need_match[x]["other"])
    info_columns = ["additional_info",
                    "Add any additional detail about what you need here"]
    english_requests["Additional Information"] = english_requests[
        info_columns].apply(
        lambda row: "\n".join(row.values.astype(str)).strip(), axis=1)
    english_requests.fillna("", inplace=True)
    english_requests.drop(info_columns + ["What do you need?"], inplace=True,
                          axis=1)
    return english_requests


sheet_yaml = yml("migration/sheets.yaml")
request_df = read_pickle("sheets.pickle")
english_requests_object, spanish_requests = break_out_language_requests(
    request_df=request_df)
english_requests_df = apply_needs_info(english_requests_object)
print(english_requests_df.columns)

atRequests = Airtable(base=yaml["airtable"]["base"],
                      api_key=yaml["airtable"]["api_key"],
                      table="Requests")

zip_dictionary = get_city_dict()
for index, row in english_requests_df.head(20).iterrows():
    needs = row["needs_list"]
    migrated_needs = list()
    for need_string in needs:
        migrated_needs.append(
            sheet_yaml["sheets_needs_to_airtable"][need_string])
    # print(migrated_needs)
    request_zip = row[
        "What's your zip code? (If you don't know, write your city and neighborhood)."]
    try:
        abc = zipcodes.matching(str(request_zip))
        zip_thing = [zip_dictionary[str(request_zip)]]
    except BaseException as e:
        zip_thing = None

    information_object = "\n".join(
        ["*****",
         "Contact:",
         str(row["How do you prefer to be contacted?"]),
         "*****",
         "Phone:",
         str(row["What's your phone number?"]),
         "*****",
         "Email Address:",
         str(row["What's your email?"]),
         "*****",
         "Contact:",
         str(row["How do you prefer to be contacted?"]),
         "*****",
         "Location:",
         str(row["What's your zip code? (If you don't know, write "
                 "your city and neighborhood)."]),
         "*****",
         "Can you leave your home?:",
         str(row["Can you leave your home?"]),
         "*****",
         "Language:",
         str(row["language"]),
         "*****",
         "Details:",
         str(row["Additional Information"])
         ])

    airtable_object = {"Name": row["What's your name?"],
                       "Email Address": row["What's your email?"],
                       "Phone": row["What's your phone number?"],
                       "Zip Code": zip_thing,
                       "Service Requested": migrated_needs,
                       "Details": information_object}
    try:
        airtable_object["Phone"] = phonenumbers.format_number(
            phonenumbers.parse(
                number=str(airtable_object["Phone"]), region="US"),
            phonenumbers.PhoneNumberFormat.NATIONAL)
        atRequests.insert_json(airtable_object)
    except (HTTPError, TypeError,
            phonenumbers.phonenumberutil.NumberParseException) as e:
        # print(e)
        airtable_object.pop("Phone")
        atRequests.insert_json(airtable_object)
    pprint(airtable_object)

airtable_requests = get_airtable_df("Requests")
print(airtable_requests.columns)

for index, row in airtable_requests.iterrows():
    if row["Ticket"] >= 16:
        pass
        # atRequests.delete(row["id"])


# airtable_services = get_airtable_df("Services and Skills")
# print(airtable_services.columns)
# pretty_print(airtable_services.head(100))
