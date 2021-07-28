""" Driver for the ridesystems report scraper"""
from datetime import date, datetime, timedelta
from typing import Optional, Union

import pandas as pd  # type: ignore
import sqlalchemy.orm  # type: ignore
from loguru import logger
from ridesystems.reports import Reports
from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.orm import Session  # type: ignore

from .creds import RIDESYSTEMS_USERNAME, RIDESYSTEMS_PASSWORD
from .schema import Base, CirculatorArrival, CirculatorBusRuntimes, CirculatorRidership
from .._merge import insert_or_update


class RidesystemReports:
    """Populates data from the Ridesystems API into the database"""

    def __init__(self, conn_str: str, rs_user: Optional[str] = None, rs_pass: Optional[str] = None):
        """
        :param conn_str: Database connection string
        :param rs_user: Ridesystems username
        :param rs_pass: Ridesystems password
        """
        if rs_user is None:
            rs_user = RIDESYSTEMS_USERNAME
        if rs_pass is None:
            rs_pass = RIDESYSTEMS_PASSWORD
        self.rs_cls = Reports(rs_user, rs_pass)

        self.engine = create_engine(conn_str, echo=True, future=True)
        with self.engine.begin() as connection:
            Base.metadata.create_all(connection)

    def get_otp(self, start_date: date, end_date: date, force: bool = False, **kwargs) -> None:
        """
        Gets the data from the ride systems scraper and puts it in the database

        :param start_date: First date (inclusive) to write to the database
        :param end_date: Last date (inclusive) to write to the database
        :param force: Regenerate the data for the date range. By default, it skips dates with existing data.
        :param kwargs: passed directly to ridesystems.get_otp
        """
        logger.info("Processing on time%: {} to {}", start_date.strftime('%m/%d/%y'), end_date.strftime('%m/%d/%y'))

        dates_to_process = self.get_dates_to_process(start_date, end_date, CirculatorArrival.date, force)

        for search_date in dates_to_process:
            logger.info('Processing {}', search_date)
            for _, row in self.rs_cls.get_otp(search_date, search_date, **kwargs).iterrows():
                actualarrivaltime = row['actualarrivaltime'] if row['actualarrivaltime'] is not pd.NaT else None
                actualdeparturetime = row['actualdeparturetime'] if row['actualdeparturetime'] is not pd.NaT else None
                vehicle = row['actualarrivaltime'] if row['actualarrivaltime'] == 'nan' else None

                insert_or_update(CirculatorArrival(
                    date=row['date'],
                    route=row['route'],
                    stop=row['stop'],
                    block_id=row['blockid'],
                    scheduled_arrival_time=row['scheduledarrivaltime'],
                    actual_arrival_time=actualarrivaltime,
                    scheduled_departure_time=row['scheduleddeparturetime'],
                    actual_departure_time=actualdeparturetime,
                    on_time_status=row['ontimestatus'],
                    vehicle=vehicle), self.engine)

    def get_vehicle_assignments(self, start_date: date, end_date: date, force: bool = False) -> None:
        """
        Gets the vehicle runtime information from ridesystems and inserts it in the database

        :param start_date: First date (inclusive) to write to the database
        :param end_date: Last date (inclusive) to write to the database
        :param force: Regenerate the data for the date range. By default, it skips dates with existing data.
        """
        logger.info("Processing bus arrivals: {} to {}", start_date.strftime('%m/%d/%y'), end_date.strftime('%m/%d/%y'))
        dates_to_process = self.get_dates_to_process(start_date, end_date, CirculatorBusRuntimes.starttime, force)
        for search_date in dates_to_process:
            logger.info('Processing {}', search_date)
            for _, row in self.rs_cls.get_runtimes(search_date, search_date).iterrows():
                insert_or_update(CirculatorBusRuntimes(
                    busid=row['vehicle'],
                    route=row['route'],
                    starttime=row['start_time'],
                    endtime=row['end_time']
                ), self.engine)

    def get_ridership(self, start_date: date, end_date: date, force: bool = False) -> None:
        """
        Gets the ridership data from ridesystems and inserts it into the database

        :param start_date: First date (inclusive) to write to the database
        :param end_date: Last date (inclusive) to write to the database
        :param force: Regenerate the data for the date range. By default, it skips dates with existing data.
        """
        logger.info("Processing ridership: {} to {}", start_date.strftime('%m/%d/%y'), end_date.strftime('%m/%d/%y'))
        dates_to_process = self.get_dates_to_process(start_date, end_date, CirculatorBusRuntimes.starttime, force)
        for search_date in dates_to_process:
            logger.info('Processing {}', search_date)
            for _, row in self.rs_cls.get_ridership(search_date, search_date).iterrows():
                insert_or_update(CirculatorRidership(
                    vehicle=row['vehicle'],
                    route=row['route'],
                    stop=row['stop'],
                    latitude=row['latitude'],
                    longitude=row['longitude'],
                    datetime=row['datetime'],
                    boardings=row['entries'],
                    alightings=row['exits'],
                ), self.engine)

    def get_dates_to_process(self, start_date: date, end_date: date, column: sqlalchemy.column,
                             force: bool = False) -> list:
        """

        :param start_date: First date (inclusive) to write to the database
        :param end_date: Last date (inclusive) to write to the database
        :param column: sqlalchemy date column to search for matching dates to skip
        :param force: Regenerate the data for the date range. By default, it skips dates with existing data.
        """
        def _convert_to_date(dte: Union[date, datetime]):
            if isinstance(dte, datetime):
                try:
                    return dte.date()
                except AttributeError:
                    pass
            if isinstance(dte, date):
                return dte
            raise AssertionError("Unknown type of date: {}".format(dte))
        with Session(bind=self.engine, future=True) as session:
            if not force:
                existing_dates = set(_convert_to_date(i[0]) for i in session.query(column).all())
            else:
                existing_dates = set()

            expected_dates = {start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)}
            dates_to_process = list(expected_dates - existing_dates)
            dates_to_process.sort(reverse=True)

            return dates_to_process
