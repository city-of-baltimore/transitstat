"""Imports ridership data from the spreadsheets that are sent monthly"""
import os
import re
import shutil
from datetime import datetime
from math import isnan
from pathlib import Path
from typing import Optional

import pandas as pd  # type: ignore
from loguru import logger
from sqlalchemy import create_engine, inspect as sqlalchemyinspect  # type: ignore
from sqlalchemy.exc import IntegrityError  # type: ignore
from sqlalchemy.ext.declarative import DeclarativeMeta  # type: ignore
from sqlalchemy.orm import Session  # type: ignore
from sqlalchemy.sql import text  # type: ignore

from .schema import Base, CirculatorRidership


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
                            self._insert_or_update(CirculatorRidership(RidershipDate=bus_date.date(),
                                                                       Route=row['Route'],
                                                                       BlockID=re.sub('[^0-9]', '', row['Block']),
                                                                       Riders=row[bus_date]))
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
            dst_filename = '{}_{}'.format(os.path.join(processed_dir, os.path.basename(file_name)), i)
            if not os.path.exists(os.path.join(processed_dir, dst_filename)):
                shutil.move(file_name, dst_filename)
                return True
            i += 1

        logger.error('Error moving file. It will not be moved to the processed directory: {}', file_name)
        return False

    def _insert_or_update(self, insert_obj: DeclarativeMeta, identity_insert=False) -> None:
        """
        A safe way for the sqlalchemy to insert if the record doesn't exist, or update if it does. Copied from
        trafficstat.crash_data_ingester
        :param insert_obj:
        :param identity_insert:
        :return:
        """
        session = Session(bind=self.engine, future=True)
        if identity_insert:
            session.execute(text('SET IDENTITY_INSERT {} ON'.format(insert_obj.__tablename__)))

        session.add(insert_obj)
        try:
            session.commit()
            logger.debug('Successfully inserted object: {}', insert_obj)
        except IntegrityError as insert_err:
            session.rollback()

            if '(544)' in insert_err.args[0]:
                # This is a workaround for an issue with sqlalchemy not properly setting IDENTITY_INSERT on for SQL
                # Server before we insert values in the primary key. The error is:
                # (pyodbc.IntegrityError) ('23000', "[23000] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]
                # Cannot insert explicit value for identity column in table <table name> when IDENTITY_INSERT is set to
                # OFF. (544) (SQLExecDirectW)")
                self._insert_or_update(insert_obj, True)

            elif '(2627)' in insert_err.args[0] or 'UNIQUE constraint failed' in insert_err.args[0]:
                # Error 2627 is the Sql Server error for inserting when the primary key already exists. 'UNIQUE
                # constraint failed' is the same for Sqlite
                cls_type = type(insert_obj)

                qry = session.query(cls_type)

                primary_keys = [i.key for i in sqlalchemyinspect(cls_type).primary_key]
                for primary_key in primary_keys:
                    qry = qry.filter(cls_type.__dict__[primary_key] == insert_obj.__dict__[primary_key])

                update_vals = {k: v for k, v in insert_obj.__dict__.items()
                               if not k.startswith('_') and k not in primary_keys}
                if update_vals:
                    qry.update(update_vals)
                    try:
                        session.commit()
                        logger.debug('Successfully inserted object: {}', insert_obj)
                    except IntegrityError as update_err:
                        logger.error('Unable to insert object: {}\nError: {}', insert_obj, update_err)

            else:
                raise AssertionError('Expected error 2627 or "UNIQUE constraint failed". Got {}'.format(insert_err)) \
                    from insert_err
        finally:
            if identity_insert:
                session.execute(text('SET IDENTITY_INSERT {} OFF'.format(insert_obj.__tablename__)))
            session.close()
