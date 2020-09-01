"""
Read the Harbor Connector ridership from an excel spreadsheet and put it in the database

CREATE TABLE [dbo].[hc_ridership](
    [route_id] [int] NOT NULL,
    [date] [date] NOT NULL,
    [riders] [int] NOT NULL
)
"""
import argparse
import glob
import logging
import math
import os
import re
import traceback

import pandas as pd
from dateutil import parser
import pyodbc

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def parse_sheets(path):
    """
    Parse the specified file or directory for all of the ridership data in each of its sheets, and return a dictionary
    of values with date (yyyy-mm-dd) -> total ridership. If a directory is provided, then all xlsx files will be
    processed

    :param path: Path of the file or directory to parse
    :type path: str
    :return ridership: (dict) The dates and ridership numbers of the Harbor Connector from parse_sheet()
    {<routeid> : {<date>, <ridership>, ...},
     ...
    }
    """
    logging.info("Processing %s", path)
    if os.path.isdir(path):
        ret = {}
        for hc_file in glob.glob(os.path.join(path, 'HC* *-*.xlsx')):
            route_id, ridership = _parse_sheets(hc_file)
            ridership.update(ret.setdefault(route_id, {}))
            ret[route_id] = ridership
        return ret
    route_id, ridership = _parse_sheets(path)
    return {route_id: ridership}


def _parse_sheets(filename):
    route_id = re.search(r'HC(\d*) \d{1,2}-\d{4}.xlsx', filename).group(1)
    if not route_id.isdigit():
        logging.error("Unable to get route from %s", filename)
    sheets_dict = pd.read_excel(filename, sheet_name=None)

    ridership = {}

    for _, sheet in sheets_dict.items():
        for _, row in sheet.iterrows():
            if isinstance(row['Created on'], float) and math.isnan(row['Created on']):
                break
            try:
                created_on = row['Created on'].replace('Thur', 'Thu')
                rider_date = parser.parse(created_on).strftime('%Y-%m-%d')
            except parser._parser.ParserError:  # pylint:disable=protected-access
                logging.error("Parse failure. %s", traceback.format_exc())
                continue

            if isinstance(row.get('Boarding'), str) and row.get('Boarding').isdigit():
                boarding = int(row.get('Boarding'))
            elif isinstance(row.get('Boarding'), (int, float)):
                boarding = int(row.get('Boarding'))
            else:
                boarding = 0
            ridership[rider_date] = ridership.setdefault(rider_date, 0) + boarding

    logging.info("Route id: %s, ridership: %s", route_id, ridership)
    return route_id, ridership


def insert_into_db(parsed_data):
    """
    Insert the ridership dictionary into the database

    :param parsed_data: The route, dates and ridership numbers of the Harbor Connector from parse_sheet()
    :type parsed_data: dict
    :return: None
    """
    conn = pyodbc.connect(r'Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
    cursor = conn.cursor()

    insert_array = []
    for route_id, ridership in parsed_data.items():
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
    aparser.add_argument('-p', '--path',
                         help='File or directory to import. If directory is provided, then all files will be processed')

    args = aparser.parse_args()

    ridership = parse_sheets(args.path)
    insert_into_db(ridership)


if __name__ == '__main__':
    start_from_cmdline()
