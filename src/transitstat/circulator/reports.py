""" Driver for the ridesystems report scraper"""
from datetime import date, timedelta
from typing import Optional

import pandas as pd  # type: ignore
from loguru import logger
from ridesystems.reports import Reports
from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.orm import Session  # type: ignore

from .creds import RIDESYSTEMS_USERNAME, RIDESYSTEMS_PASSWORD
from .schema import Base, CirculatorArrival
from .._merge import insert_or_update


def get_otp(start_date: date, end_date: date, conn_str: str,  # pylint:disable=too-many-locals, too-many-arguments
            force: bool = False, rs_user: Optional[str] = None, rs_pass: Optional[str] = None) -> None:
    """
    Gets the data from the ride systems scraper and puts it in the database

    :param start_date: First date (inclusive) to write to the database
    :param end_date: Last date (inclusive) to write to the database
    :param conn_str: Database connection string
    :param force: Regenerate the data for the date range. By default, it skips dates with existing data.
    :param rs_user: Ridesystems username
    :param rs_pass: Ridesystems password
    """
    logger.info("Processing bus arrivals: {} to {}", start_date.strftime('%m/%d/%y'), end_date.strftime('%m/%d/%y'))
    if rs_user is None:
        rs_user = RIDESYSTEMS_USERNAME
    if rs_pass is None:
        rs_pass = RIDESYSTEMS_PASSWORD

    logger.info("usingusing{}and{}", rs_user, rs_pass)
    rs_cls = Reports(rs_user, rs_pass)

    engine = create_engine(conn_str, echo=True, future=True)
    with engine.begin() as connection:
        Base.metadata.create_all(connection)

    with Session(bind=engine, future=True) as session:
        if not force:
            existing_dates = set(i[0] for i in session.query(CirculatorArrival.date).all())
        else:
            existing_dates = set()

        expected_dates = {start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)}
        dates_to_process = list(expected_dates - existing_dates)
        dates_to_process.sort(reverse=True)

        for search_date in dates_to_process:
            logger.info('Processing {}', search_date)
            for _, row in rs_cls.get_otp(search_date, search_date).iterrows():
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
                    vehicle=vehicle), engine)
