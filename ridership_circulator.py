""" Pulls the ridership data from the Ridesystems API and put it in our database"""
import argparse
from datetime import datetime, timedelta
from collections import defaultdict

import pyodbc
import shuttle

CONN = pyodbc.connect(r'Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
CURSOR = CONN.cursor()

def insert_data(day, month, year, numofdays):
    """
    Insert the ridership date from the Ridesystems API into our database.

    :param day: Day of date we should aggregate (IE: 5)
    :type day: int
    :param month: Month of date we should aggregate (IE: 10 for Oct)
    :type month: int
    :param year: Year of date we should aggregate (IE: 2020)
    :type year: int
    :param numofdays: Number of days (moving backwards) to aggregate
    :type numofdays: int
    :return: None
    """

    #CURSOR.execute("""
    #CREATE TABLE [dbo].[ccc_aggregate_ridership](
    #[RidershipDate] [date] NULL,
    #[VehicleID] [int] NULL,
    #[RouteStopID] [int] NULL,
    #[Route] [nchar](30) NULL,
    #[Weekday] [int] NULL,
    #[Boardings] [int] NULL,
    #[Alightings] [int] NULL
    #);
    #""")
    #CURSOR.commit()

    for i in range(numofdays):

        end = datetime(year, month, day, 23, 59, 59, 000000) - timedelta(days=i)
        start = datetime(year, month, day, 00, 00, 00, 000000) - timedelta(days=i)
        print("Processing {}".format(start.strftime("%Y-%m-%d %H:%M:%S")))

        ridership_data = shuttle.get_ridership_data(start_date=start.strftime("%Y-%m-%d %H:%M:%S"),
                                                    end_date=end.strftime("%Y-%m-%d %H:%M:%S"))

        ridership = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: list([0, 0]))))

        for stop_data in ridership_data:
            if stop_data['RouteStopID'] == 0:
                continue
            #clean the route name
            if 'purple' in stop_data['Route'].lower():
                route = 'purple'
            elif 'green' in stop_data['Route'].lower():
                route = 'green'
            elif 'orange' in stop_data['Route'].lower():
                route = 'orange'
            elif 'banner' in stop_data['Route'].lower():
                route = 'banner'
            ridership[route][stop_data['VehicleID']][stop_data['RouteStopID']][0] += stop_data['Entries']
            ridership[route][stop_data['VehicleID']][stop_data['RouteStopID']][1] += stop_data['Exits']

        entries = 0
        exits = 0
        data = []
        for route, x in ridership.items():
            for vehicleid, y in x.items():
                for stopid, z in y.items():
                    data.append((start.strftime("%Y-%m-%d"), vehicleid, stopid, route, start.weekday(), z[0], z[1]))

        if not data:
            continue

        CURSOR.executemany("""
        MERGE ccc_aggregate_ridership USING (
        VALUES
            (?, ?, ?, ?, ?, ?, ?)
        ) AS vals (RidershipDate, VehicleID, RouteStopID, Route, Weekday, Boardings, Alightings)
        ON (ccc_aggregate_ridership.RidershipDate = vals.RidershipDate AND
            ccc_aggregate_ridership.VehicleID = vals.VehicleID AND
            ccc_aggregate_ridership.RouteStopID = vals.RouteStopID AND
            ccc_aggregate_ridership.Route = vals.Route)
        WHEN MATCHED THEN
            UPDATE SET
            Boardings = vals.Boardings,
            Alightings = vals.Alightings
        WHEN NOT MATCHED THEN
            INSERT (RidershipDate, VehicleID, RouteStopID, Route, Weekday, Boardings, Alightings)
            VALUES (vals.RidershipDate, vals.VehicleID, vals.RouteStopID, vals.Route, vals.Weekday, vals.Boardings, vals.Alightings);
        """, data)

        CURSOR.commit()


def start_from_cmdline():
    """
    Parse the args and ki
    """
    today = datetime.now()
    parser = argparse.ArgumentParser(description='Circulator ridership aggregator')
    parser.add_argument('-m', '--month', default=today.month, type=int,
                        help='Month of date we should aggregate (IE: 10 for Oct)')
    parser.add_argument('-d', '--day', default=today.day, type=int,
                        help='Day of date we should aggregate (IE: 5)')
    parser.add_argument('-y', '--year', default=today.year, type=int,
                        help='Year of date we should aggregate (IE: 2020)')
    parser.add_argument('-n', '--numofdays', default=1, type=int,
                        help='Number of days in reverse to aggregate')

    args = parser.parse_args()
    insert_data(args.day, args.month, args.year, args.numofdays)

if __name__ == '__main__':
    start_from_cmdline()
