""" Driver for the ridesystems report scraper

CREATE TABLE [dbo].[ccc_arrival_times2](
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
import logging
from datetime import timedelta

import pyodbc  # type: ignore
from ridesystems.reports import Reports

from circulator.creds import RIDESYSTEMS_USERNAME, RIDESYSTEMS_PASSWORD

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

conn = pyodbc.connect(r'Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
cursor = conn.cursor()


def update_database(start_date, end_date):
    """Gets the data from the ride systems scraper and puts it in the database"""
    logging.info("Processing %s to %s", start_date.strftime('%m/%d/%y'), end_date.strftime('%m/%d/%y'))
    rs_cls = Reports(RIDESYSTEMS_USERNAME, RIDESYSTEMS_PASSWORD)

    data = []
    for search_date in date_range(start_date, end_date):
        for row in rs_cls.get_otp(search_date, search_date):
            data.append((row['date'], row['route'], row['stop'], row['blockid'], row['scheduledarrivaltime'],
                         row['actualarrivaltime'], row['scheduleddeparturetime'], row['actualdeparturetime'],
                         row['ontimestatus'], row['vehicle']))

        if data:
            cursor.executemany("""
                MERGE [ccc_arrival_times2] USING (
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ) AS vals (date, route, stop, blockid, scheduledarrivaltime, actualarrivaltime, scheduleddeparturetime,
                actualdeparturetime, ontimestatus, vehicle)
                ON (ccc_arrival_times2.date = vals.date AND
                    ccc_arrival_times2.route = vals.route AND
                    ccc_arrival_times2.blockid = vals.blockid AND
                    ccc_arrival_times2.stop = vals.stop)
                WHEN MATCHED THEN
                    UPDATE SET
                    scheduledarrivaltime = vals.scheduledarrivaltime,
                    actualarrivaltime = vals.actualarrivaltime,
                    scheduleddeparturetime = vals.scheduleddeparturetime,
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


def date_range(start_date, end_date):
    """Helper to iterate over dates"""
    for i in range(int((end_date - start_date).days)):
        yield start_date + timedelta(i)
