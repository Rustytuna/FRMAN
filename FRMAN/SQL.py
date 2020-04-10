#!/usr/bin/env python3

# Author::    Justin Flannery  (mailto:juftin@juftin.com)

"""
COMMUNICATION WITH SQLITE DB.
"""

from pandas import DataFrame
from pandas import read_sql as _read_sql
from sqlalchemy import create_engine


class SQL:
    """Main SQL Class."""

    def __init__(self, db_file):
        """RETURN SQLITE ENGINE."""
        db_uri = "sqlite:///" + db_file
        engine = create_engine(db_uri)
        self.engine = engine

    def ingest_df(self, dataframe, table_name, if_exists="append",
                  careful=False, key=None):
        """
        INGEST DATAFRAME TO SQLITE DB.
        """
        if not dataframe.empty:
            conn = self.engine.connect()
            if careful is True and key is not None:
                for index, row in dataframe.iterrows():
                    delete = "DELETE FROM " + table_name + \
                             " WHERE " + key + " = '" + str(row[key]) + "'"
                    conn.execute(delete)
            dataframe.to_sql(table_name, con=conn,
                             if_exists=if_exists, index=False)
            conn.close()
            length = len(dataframe)
        else:
            length = 0
        return length

    def read_sql(self, query: object) -> object:
        """
        SELECT * FROM SQLITE TABLE INTO DF.
        """
        conn = self.engine.connect()
        dataframe = _read_sql(query, con=conn)
        conn.close()
        return dataframe

    def vacuum(self):
        """
        VACUUM SQLITE DATABASE.
        """
        conn = self.engine.connect()
        conn.execute("VACUUM")
        conn.close()

    def get_var(self, table, var, where=None):
        """
        GET VARIABLE FROM SQL DATABASE.
        """
        conn = self.engine.connect()
        if where is None:
            where = "1=1"
        if "MAX" not in var:
            var = '"' + var + '"'
        where_statement = "SELECT DISTINCT " + \
                          var + " FROM " + table + " WHERE " + where
        result = conn.execute(where_statement)
        try:
            variable = result.fetchone()[0]
        except TypeError as e:
            print(e)
            variable = None
        conn.close()
        return variable

    def delete(self, table, key, column):
        """
        DELETE DATA FROM SQL DATABASE.
        """
        conn = self.engine.connect()
        key = "'" + key + "'"
        delete = "DELETE FROM " + table + ' WHERE ' + column + \
                 " = " + str(key)
        conn.execute(delete)
        conn.close()

    def execute(self, statement):
        """
        EXECUTE STATEMENT ON DATABASE.
        """
        conn = self.engine.connect()
        conn.execute(statement)
        conn.close()

    @staticmethod
    def to_csv(dataframe, file_path, header=True, index=False, delimiter=",",
               encoding="utf-8", quotechar='"', quoting=1):
        """
        STANDARDIZED CSV EXPORT.
        """
        dataframe.to_csv(file_path, header=header, index=index, sep=delimiter,
                         encoding=encoding, quotechar=quotechar,
                         quoting=quoting)

    def get_dict(self, table, key, value, data=False):
        """
        GET DICTIONARY FROM DATAFRAME WITH 2 KEY, VALUE COLUMNS.
        """
        if data is False:
            conn = self.engine.connect()
            dataframe = _read_sql(
                "SELECT {}, {} FROM {}".format(key, value, table), con=conn)
            conn.close()
        elif type(data) == DataFrame:
            dataframe = data
        dataframe.set_index(key, inplace=True)
        df_dict = dataframe.to_dict(orient="index")
        for key, item in df_dict.items():
            df_dict[key] = item[value]
        return df_dict
