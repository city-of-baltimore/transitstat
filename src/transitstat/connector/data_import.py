"""Read the Harbor Connector ridership from an excel spreadsheet and put it in the database"""
import math
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Dict, Optional, Tuple

from loguru import logger
import pandas as pd  # type: ignore
from dateutil import parser as date_parser
from dateutil.parser import ParserError
from sqlalchemy import create_engine  # type: ignore

from transitstat.args import setup_logging, setup_parser
from .schema import Base, HcRidership
from .._merge import insert_or_update

RidershipDict = Dict[date, int]
ParsedDataDict = Dict[int, RidershipDict]


class ConnectorImport:
    """Imports ridership data from the Harbor Connector reports"""
    def __init__(self, conn_str):
        """
        :param conn_str: Connection string passed to SqlAlchemy
        """
        self.engine = create_engine(conn_str, echo=True, future=True)
        with self.engine.begin() as connection:
            Base.metadata.create_all(connection)

    def parse_sheets(self, path: Path) -> ParsedDataDict:
        """
        Parse the specified file or directory for all of the ridership data in each of its sheets, and return a
        dictionary of values with date (yyyy-mm-dd) -> total ridership. If a directory is provided, then all xlsx files
        will be processed

        :param path: Path of the file or directory to parse
        :return ridership: (dict) The dates and ridership numbers of the Harbor Connector from parse_sheet()
        {<routeid> : {<date>, <ridership>, ...},
         ...
        }
        """
        logger.info('Processing {}', path)
        ret: ParsedDataDict = defaultdict(dict)
        file_list = path.glob('HC* *-*.xlsx') if path.exists() else [path]

        for hc_file in file_list:
            parsed = self._parse_sheets(hc_file)
            if parsed:
                route_id, ridership = parsed
                ridership.update(ret[route_id])
                ret[route_id] = ridership
        return ret

    @staticmethod
    def _parse_sheets(filename: Path) -> Optional[Tuple[int, Dict]]:
        def _count_converter(val):
            try:
                val = int(val)
            except ValueError:
                return 0

            if math.isnan(val):
                return 0
            return val

        filename_parse = re.search(r'HC(\d*) \d{1,2}-\d{4}.xlsx', str(filename))
        if not filename_parse:
            return None

        route_id: int = int(filename_parse.group(1))
        sheets_dict = pd.read_excel(filename, sheet_name=None,
                                    converters={'Count': _count_converter, 'Boardings': _count_converter},
                                    dtype={'Date': str,
                                           'Depart Location': str,
                                           'Boardings': int})
        ridership: RidershipDict = {}

        for _, sheet in sheets_dict.items():
            for _, row in sheet.iterrows():
                if isinstance(row['Date'], float) and math.isnan(row['Date']):
                    break
                try:
                    created_on = row['Date']
                    # .replace('Thur', 'Thu')\
                    # .replace('(Eastern Daylight Time)', '(EDT)')\
                    # .replace('(Eastern Standard Time)', '(EST)')
                    rider_date: date = date_parser.parse(created_on).date()
                except ParserError as err:
                    logger.error('Parse failure. {}', err)
                    continue

                if isinstance(row.get('Boardings'), str) and row.get('Boardings').isdigit():
                    boarding = int(row.get('Boardings'))
                elif isinstance(row.get('Boardings'), (int, float)) and not math.isnan(row.get('Boardings')):
                    boarding = int(row.get('Boardings'))
                else:
                    boarding = 0
                ridership[rider_date] = ridership.setdefault(rider_date, 0) + boarding

        logger.info('Route id: {}, ridership: {}', route_id, ridership)
        return route_id, ridership

    def insert_into_db(self, parsed_data: ParsedDataDict) -> None:
        """
        Insert the ridership dictionary into the database

        :param parsed_data: The route, dates and ridership numbers of the Harbor Connector from parse_sheet()
        :type parsed_data: dict
        :return: None
        """
        for route_id, ridership in parsed_data.items():
            for _date, riders in ridership.items():
                insert_or_update(HcRidership(route_id=route_id,
                                             date=_date,
                                             riders=riders), self.engine)


def parse_args(args):
    """Handles argument parsing"""
    parser = setup_parser('Driver for the Harbor Connector scripts')

    parser.add_argument('-p', '--path',
                        help='File or directory to import. If directory is provided, then all files will be processed')

    return parser.parse_args(args)


if __name__ == '__main__':
    _args = parse_args(sys.argv[1:])
    setup_logging(_args.debug, _args.verbose)
    clss = ConnectorImport(_args.conn_str)
    clss.insert_into_db(clss.parse_sheets(Path(_args.path)))
