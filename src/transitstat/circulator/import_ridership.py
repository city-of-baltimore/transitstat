"""Imports ridership data from the spreadsheets that are sent monthly"""
import os
import re
import shutil
import sys
from datetime import datetime
from math import isnan
from pathlib import Path
from typing import Optional

import pandas as pd  # type: ignore
from loguru import logger
from sqlalchemy import create_engine  # type: ignore

from transitstat.args import setup_logging, setup_parser
from .schema import Base, CirculatorRidershipXLS
from .._merge import insert_or_update


class DataImporter:  # pylint:disable=too-few-public-methods
    """Imports ridership data from xlsx files"""

    def __init__(self, conn_str: str):
        """
        :param conn_str: sqlalchemy connection string (IE sqlite:///crash.db or
        Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;)
        """
        logger.info('Creating db with connection string: {}', conn_str)
        self.engine = create_engine(conn_str, echo=True, future=True)
        self.conn_str = conn_str

        with self.engine.begin() as connection:
            Base.metadata.create_all(connection)

    def import_ridership(self, file: Optional[Path] = None, directory: Optional[Path] = None) -> bool:
        """
        Imports the ridership from the spreadsheet that Ridesystems sends monthly

        :param file: Either a Path or a string with the path to the xlsx file to import
        :param directory: Either a Path or a string with the path to a directory containing xlsx files to import. They
        will be moved to a .processed directory after being imported
        """
        if directory:
            logger.info('Processing directory {}', directory)
            for xlsx in directory.glob('*.xlsx'):
                if self.import_ridership(xlsx) and not self._file_move(xlsx, directory / '.processed'):
                    return False

        if file:
            logger.info('Processing file {}', file)
            dataframes = pd.read_excel(file, skiprows=[0, 1, 2, 3, 4, 5, 6], skipfooter=2,
                                       sheet_name=None)
            for key, dataframe in dataframes.items():
                if key.lower().startswith('summary') or key.lower().startswith('sheet'):
                    logger.warning('Skipping sheet name {}', key)
                    continue

                dataframe.dropna(axis=1, how='all', inplace=True, thresh=4)
                dataframe.rename(columns={dataframe.columns[0]: 'Route', dataframe.columns[1]: 'Block'}, inplace=True)
                cols = ['Route', 'Block']
                dataframe.loc[:, cols] = dataframe.loc[:, cols].ffill()
                dataframe.iloc[:, 2:] = dataframe.iloc[:, 2:].astype(int, errors='ignore')

                if not any(isinstance(i, datetime) for i in dataframe.columns):
                    logger.error('Expected data columns, and did not find any.\nFile: {}\nSheet: {}', file, key)
                    return False

                for _, row in dataframe.iterrows():
                    if row['Block'] == 'Total' or (isinstance(row['Block'], float) and isnan(row['Block'])):
                        continue

                    for bus_date in dataframe.columns[2:]:
                        if (isinstance(row[bus_date], float) and isnan(row[bus_date])) or \
                                not isinstance(bus_date, datetime):
                            continue

                        if isinstance(row[bus_date], (int, float)):
                            insert_or_update(CirculatorRidershipXLS(RidershipDate=bus_date.date(),
                                                                    Route=row['Route'],
                                                                    BlockID=int(re.sub('[^0-9]', '', row['Block'])),
                                                                    Riders=int(row[bus_date])), self.engine)
        return True

    @staticmethod
    def _file_move(file_name: Path, processed_dir: Path) -> bool:
        """
        File copy with automatic renaming during retry
        :param file_name: File to copy to processed_dir
        :param processed_dir: Directory to copy file into
        """
        if not os.path.exists(processed_dir):
            os.mkdir(processed_dir)

        if not os.path.exists(os.path.join(processed_dir, os.path.basename(file_name))):
            shutil.move(file_name, processed_dir)
            return True

        # Otherwise we need to figure out another filename
        i = 1
        while i < 6:
            # retry copy operation up to 5 times
            dst_filename = f'{os.path.join(processed_dir, os.path.basename(file_name))}_{i}'
            if not os.path.exists(os.path.join(processed_dir, dst_filename)):
                shutil.move(file_name, dst_filename)
                return True
            i += 1

        logger.error('Error moving file. It will not be moved to the processed directory: {}', file_name)
        return False


def parse_args(args):
    """Handles argument parsing"""
    parser = setup_parser('Imports ridership data from a standard XLSX file')

    # Import harbor connector file
    parser.add_argument('-f', '--file', help='File to import')
    parser.add_argument('-d', '--dir', help='Directory to process that contains XLSX files with ridership data')

    return parser.parse_args(args)


if __name__ == '__main__':
    parsed_args = parse_args(sys.argv[1:])
    setup_logging(parsed_args.debug, parsed_args.verbose)

    # Import ridership
    di = DataImporter(parsed_args.conn_str)
    if parsed_args.file:
        di.import_ridership(file=Path(parsed_args.file))

    if parsed_args.dir:
        di.import_ridership(directory=Path(parsed_args.dir))
