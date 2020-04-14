"""FUNCTIONS FOR AIRTABLE INTEGRATION."""

from airtable import Airtable as _airtable
from pandas import DataFrame
from pandas.io.json import json_normalize


class Airtable:
    """Interactions With AirTable."""

    def __init__(self, base, table, api_key):
        """RETURN AIRTABLE CONNECTION.
        :rtype: object
        """
        self.base = base
        self.table = table
        airtable = _airtable(base, table, api_key=api_key)
        self.airtable_conn = airtable

    def get_airtable_raw(self, params=None):
        """Get Data From AirTable."""
        results = self.airtable_conn.get_all(filterByFormula=params)
        dataframe = DataFrame(json_normalize(results))
        dataframe.rename(columns=lambda x: x.replace(
            "fields.", ""), inplace=True)
        return dataframe

    def insert_json(self, json: dict):
        """
        INSERT RECORDS INTO AIRTABLE.
        """
        result = self.airtable_conn.insert(json, typecast=True)
        return result

    def update(self, record_id, fields, typecast=True, run=True):
        """UPDATE RECORDS BY ID AND DICT."""
        self.airtable_conn.update(record_id, fields, typecast=typecast)

    def delete(self, record_id):
        """DELETE RECORD BY ID."""
        self.airtable_conn.delete(record_id)
