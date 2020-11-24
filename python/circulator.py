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
import argparse
import logging
from datetime import date, timedelta, datetime

import creds
import pyodbc
import ridesystems  # pylint:disable=import-error # Because we don't have the wheel in github actions

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

conn = pyodbc.connect(r'Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')


def update_database(start_date, end_date):
    """Gets the data from the ride systems scraper and puts it in the database"""
    logging.info("Processing %s to %s", start_date.strftime('%m/%d/%y'), end_date.strftime('%m/%d/%y'))
    rs_cls = ridesystems.Scraper(creds.RIDESYSTEMS_USERNAME, creds.RIDESYSTEMS_PASSWORD)

    for search_date in daterange(start_date, end_date):
        for row in rs_cls.get_otp(search_date, search_date):
            conn.execute("""
            MERGE [ccc_arrival_times2] USING (
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ) AS vals (date, route, stop, blockid, scheduledarrivaltime, actualarrivaltime, scheduleddeparturetime,
            actualdeparturetime, ontimestatus, vehicle)
            ON (ccc_arrival_times2.date = vals.date AND
                ccc_arrival_times2.route = vals.route AND
                ccc_arrival_times2.blockid = vals.blockid)
            WHEN MATCHED THEN
                UPDATE SET
                scheduledarrivaltime = vals.scheduledarrivaltime,
                actualarrivaltime = vals.actualarrivaltime,
                scheduleddeparturetime = vals.scheduleddeparturetime,
                actualdeparturetime = vals.actualdeparturetime,
                stop = vals.stop,
                ontimestatus = vals.ontimestatus,
                vehicle = vals.vehicle
            WHEN NOT MATCHED THEN
                INSERT (date, route, stop, blockid, scheduledarrivaltime, actualarrivaltime, scheduleddeparturetime,
                    actualdeparturetime, ontimestatus, vehicle)
                VALUES (vals.date, vals.route, vals.stop, vals.blockid, vals.scheduledarrivaltime,
                    vals.actualarrivaltime, vals.scheduleddeparturetime, vals.actualdeparturetime, vals.ontimestatus,
                    vals.vehicle);
            """, row['date'], row['route'], row['stop'], row['blockid'], row['scheduledarrivaltime'],
                         row['actualarrivaltime'], row['scheduleddeparturetime'], row['actualdeparturetime'],
                         row['ontimestatus'], row['vehicle'])
            conn.commit()


def daterange(start_date, end_date):
    """Helper to iterate over dates"""
    for i in range(int((end_date - start_date).days)):
        yield start_date + timedelta(i)


if __name__ == '__main__':
    yesterday = date.today() - timedelta(days=1)

    parser = argparse.ArgumentParser(description="Inserts data about the circulator into the database")
    parser.add_argument("-v", "--verbose", help="Debug logging level")
    parser.add_argument('-m', '--month', type=int, default=yesterday.month,
                        help=('Optional: Month of date we should start searching on (IE: 10 for Oct). Defaults to '
                              'yesterday if not specified'))
    parser.add_argument('-d', '--day', type=int, default=yesterday.day,
                        help=('Optional: Day of date we should start searching on (IE: 5). Defaults to yesterday if '
                              'not specified'))
    parser.add_argument('-y', '--year', type=int, default=yesterday.year,
                        help=('Optional: Four digit year we should start searching on (IE: 2020). Defaults to '
                              'yesterday if not specified'))
    parser.add_argument('-n', '--numofdays', default=1, type=int,
                        help='Optional: Number of days to search, including the start date.')
    args = parser.parse_args()

    start = datetime(args.year, args.month, args.day)
    end = start + timedelta(days=args.numofdays)

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    update_database(start, end)
