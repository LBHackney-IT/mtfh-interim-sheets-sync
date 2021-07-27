"""
This module exposes functions that load external data such us from tsv files or sql server database.
"""
from typing import Dict
import pymssql


def read_db(server: str, username: str, password: str, database: str, query: str) -> [Dict]:
    """
    A function that runs a query against MSFT SQL SERVER and returns data as an array of dicts.
    :param server: The server where the source data is stored.
    :param username: The username to authenticate against the database server.
    :param password: The password to authenticate against the database server.
    :param database: The database where the data is stored.
    :param query: Query to run against SQL SERVER.
    :return: Array of dicts of the data extracted from SQL Server.
    """
    conn = pymssql.connect(host=server, user=username, password=password, database=database)
    cursor = conn.cursor(as_dict=True)

    cursor.execute(query)
    rows = cursor.fetchall()
    return rows
