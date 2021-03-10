""" Driver for the ridesystems report scraper

CREATE TABLE [dbo].[ccc_arrival_times](
    [date] [date] NOT NULL,
    [route] [varchar](50) NOT NULL,
    [stop] [varchar](max) NOT NULL,
    [blockid] [varchar](100) NOT NULL,
    [scheduledarrivaltime] [time](7) NOT NULL,
    [actualarrivaltime] [time](7) NULL,
    [scheduleddeparturetime] [time](7) NOT NULL,
    [actualdeparturetime] [time](7) NULL,
    [ontimestatus] [varchar](20) NULL,
    [vehicle] [varchar](50) NULL
)
"""
from datetime import date, timedelta

import pyodbc  # type: ignore
from loguru import logger
from ridesystems.reports import Reports

from .creds import RIDESYSTEMS_USERNAME, RIDESYSTEMS_PASSWORD

conn = pyodbc.connect(r'Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
cursor = conn.cursor()


def update_database(start_date: date, end_date: date) -> None:
    """Gets the data from the ride systems scraper and puts it in the database"""
    logger.info("Processing {} to {}", start_date.strftime('%m/%d/%y'), end_date.strftime('%m/%d/%y'))
    rs_cls = Reports(RIDESYSTEMS_USERNAME, RIDESYSTEMS_PASSWORD)

    for search_date in [start_date + timedelta(i) for i in range((end_date - start_date).days)]:
        data = []
        for row in rs_cls.get_otp(search_date, search_date):
            data.append((row['date'], row['route'], row['stop'], row['blockid'], row['scheduledarrivaltime'],
                         row['actualarrivaltime'], row['scheduleddeparturetime'], row['actualdeparturetime'],
                         row['ontimestatus'], row['vehicle']))

        if data:
            cursor.executemany("""
                MERGE [ccc_arrival_times] USING (
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ) AS vals (date, route, stop, blockid, scheduledarrivaltime, actualarrivaltime, scheduleddeparturetime,
                actualdeparturetime, ontimestatus, vehicle)
                ON (ccc_arrival_times.date = vals.date AND
                    ccc_arrival_times.route = vals.route AND
                    ccc_arrival_times.blockid = vals.blockid AND
                    ccc_arrival_times.stop = vals.stop AND
                    ccc_arrival_times.scheduledarrivaltime = vals.scheduledarrivaltime AND
                    ccc_arrival_times.scheduleddeparturetime = vals.scheduleddeparturetime)
                WHEN MATCHED THEN
                    UPDATE SET
                    actualarrivaltime = vals.actualarrivaltime,
                    actualdeparturetime = vals.actualdeparturetime,
                    ontimestatus = vals.ontimestatus,
                    vehicle = vals.vehicle
                WHEN NOT MATCHED THEN
                    INSERT (date, route, stop, blockid, scheduledarrivaltime, actualarrivaltime, scheduleddeparturetime,
                        actualdeparturetime, ontimestatus, vehicle)
                    VALUES (vals.date, vals.route, vals.stop, vals.blockid, vals.scheduledarrivaltime,
                        vals.actualarrivaltime, vals.scheduleddeparturetime, vals.actualdeparturetime, vals.ontimestatus,
                        vals.vehicle);
            """, data)
            cursor.commit()
