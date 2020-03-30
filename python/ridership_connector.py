""" Read the Harbor Connector ridership from an excel spreadsheet and put it in the database"""
import argparse
import math

import pandas as pd
from dateutil import parser
import pyodbc


def parse_sheet(filename):
    """
    Parse the specified file for all of the ridership data in each of its sheets, and return a dictionary of values
    with date (yyyy-mm-dd) -> total ridership

    :param filename: Filename of the file to parse
    :type filename: str
    :param ridership: The dates and ridership numbers of the Harbor Connector from parse_sheet()
    :type ridership: dict
    """
    sheets_dict = pd.read_excel(filename, sheet_name=None)

    ridership = {}

    for _, sheet in sheets_dict.items():
        for _, row in sheet.iterrows():
            if isinstance(row['Created on'], float) and math.isnan(row['Created on']):
                break
            rider_date = parser.parse(row['Created on']).strftime('%Y-%m-%d')
            print(rider_date, ridership, row['Boarding'])
            ridership[rider_date] = ridership.setdefault(rider_date, 0) + row['Boarding']
    return ridership

def insert_into_db(ridership, route_id):
    """
    Insert the ridership dictionary into the database

    :param ridership: The dates and ridership numbers of the Harbor Connector from parse_sheet()
    :type ridership: dict
    :param route_id: The route id of the Harbor Connector data (IE: 1, 2 or 3)
    :type ridership: int
    :return: None
    """
    conn = pyodbc.connect(r'Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
    cursor = conn.cursor()

    insert_array = []
    for date, riders in ridership.items():
        insert_array.append((route_id, date, riders, route_id, date, riders))
    cursor.executemany(("MERGE  "
                        "INTO hc_ridership WITH (HOLDLOCK) AS target "
                        "USING (SELECT "
                        "? as route_id "
                        ",? as [date]) AS source "
                        "(route_id, [date])  "
                        "ON (target.route_id = source.route_id "
                        "AND target.[date] = source.[date]) "
                        "WHEN MATCHED "
                        "THEN UPDATE "
                        "SET riders = ? "
                        "WHEN NOT MATCHED "
                        "THEN INSERT (route_id, [date], riders) "
                        "VALUES (?, ?, ?);"), insert_array)
    cursor.commit()

def start_from_cmdline():
    """
    Parse args and start
    """
    aparser = argparse.ArgumentParser(description='Harbor Connector spreadsheet importer')
    aparser.add_argument('-r', '--routeid',
                         help='The Harbor Connector route this spreadsheet tracks')
    aparser.add_argument('-f', '--file',
                         help='File to import')

    args = aparser.parse_args()

    ridership = parse_sheet(args.file)
    insert_into_db(ridership, args.routeid)

if __name__ == '__main__':
    start_from_cmdline()
