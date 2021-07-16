"""
Read the Harbor Connector ridership from an excel spreadsheet and put it in the database

CREATE TABLE [dbo].[hc_ridership](
    [route_id] [int] NOT NULL,
    [date] [date] NOT NULL,
    [riders] [int] NOT NULL
)
"""
import glob
import math
import os
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from loguru import logger
import pandas as pd  # type: ignore
from dateutil import parser
from dateutil.parser import ParserError  # type: ignore  # https://github.com/python/typeshed/pull/4616
import pyodbc  # type: ignore

RidershipDict = Dict[str, int]
ParsedDataDict = Dict[int, RidershipDict]


def parse_sheets(path: str) -> ParsedDataDict:
    """
    Parse the specified file or directory for all of the ridership data in each of its sheets, and return a dictionary
    of values with date (yyyy-mm-dd) -> total ridership. If a directory is provided, then all xlsx files will be
    processed

    :param path: Path of the file or directory to parse
    :return ridership: (dict) The dates and ridership numbers of the Harbor Connector from parse_sheet()
    {<routeid> : {<date>, <ridership>, ...},
     ...
    }
    """
    logger.info("Processing {}", path)
    ret: ParsedDataDict = defaultdict(dict)
    file_list = glob.glob(os.path.join(path, 'HC* *-*.xlsx')) if os.path.isdir(path) else [path]

    for hc_file in file_list:
        parsed = _parse_sheets(hc_file)
        if parsed:
            route_id, ridership = parsed
            ridership.update(ret[route_id])
            ret[route_id] = ridership
    return ret


def _parse_sheets(filename: str) -> Optional[Tuple[int, Dict]]:  # pylint:disable=unsubscriptable-object ; https://github.com/PyCQA/pylint/issues/3882
    filename_parse = re.search(r'HC(\d*) \d{1,2}-\d{4}.xlsx', filename)
    if not filename_parse:
        return None

    route_id: int = int(filename_parse.group(1))
    sheets_dict = pd.read_excel(filename, sheet_name=None)

    ridership: RidershipDict = {}

    for _, sheet in sheets_dict.items():
        for _, row in sheet.iterrows():
            if isinstance(row['Created on'], float) and math.isnan(row['Created on']):
                break
            try:
                created_on = row['Created on'].replace('Thur', 'Thu').replace('(Eastern Daylight Time)', '(EDT)')
                rider_date: str = parser.parse(created_on).strftime('%Y-%m-%d')
            except ParserError as err:
                logger.error("Parse failure. {}", err)
                continue

            if isinstance(row.get('Boarding'), str) and row.get('Boarding').isdigit():
                boarding = int(row.get('Boarding'))
            elif isinstance(row.get('Boarding'), (int, float)):
                boarding = int(row.get('Boarding'))
            else:
                boarding = 0
            ridership[rider_date] = ridership.setdefault(rider_date, 0) + boarding

    logger.info("Route id: {}, ridership: {}", route_id, ridership)
    return route_id, ridership


def insert_into_db(parsed_data: ParsedDataDict) -> None:
    """
    Insert the ridership dictionary into the database

    :param parsed_data: The route, dates and ridership numbers of the Harbor Connector from parse_sheet()
    :type parsed_data: dict
    :return: None
    """
    conn = pyodbc.connect(r'Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
    cursor = conn.cursor()

    insert_array: List[Tuple] = []
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
