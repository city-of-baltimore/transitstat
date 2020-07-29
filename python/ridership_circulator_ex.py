""" Pulls the ridership data from the Ridesystems API and put it in our database"""
import argparse
from datetime import datetime, timedelta
from collections import defaultdict

import pyodbc
import shuttle

CONN = pyodbc.connect(r'Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
CURSOR = CONN.cursor()


def insert_data(day, month, year, numofdays):  # pylint:disable=too-many-locals
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

    #{
    #'ClientTime': '4/5/2019 11:26:17 PM',
    #'Counter': 'Bus1210-Rear',
    #'CounterType': 'Hella',
    #'Door': (bool)
    #'Entries': 0,
    #'Exits': 2,
    #'RouteID': 2, ------->'Route': 'Purple Route_March2020',
    #'RouteStopID': 33,  ----------> 'RouteStop': '25th Street-Northbound', 'Lattitude': 39.31696, 'Longitude': -76.61703,
    #'UTCTime': '4/6/2019 3:26:17 AM',
    #'VehicleID': 16,  ----> 'Vehicle': 'CC1210'
    #}

    # CURSOR.execute("""
    # CREATE TABLE [dbo].[ccc_aggregate_ridership_ex](
    # [ClientTime] [datetime] NOT NULL,
    # [VehicleID] [int] NOT NULL,
    # [RouteStopID] [int] NOT NULL,
    # [RouteID] [int] NOT NULL,
    # [Hour] [int] NOT NULL,
    # [Weekday] [int] NOT NULL,
    # [Boardings] [int] NOT NULL,
    # [Alightings] [int] NOT NULL,
    # [Frontdoor] [bit] NOT NULL,
    # );
    # """)
    # CURSOR.commit()

    #CURSOR.execute("""
    #CREATE TABLE [dbo].[ccc_routes](
    #[RouteID] [int] NOT NULL,
    #[RouteName] [nchar](50) NOT NULL)""")

    #CURSOR.execute("""
    #CREATE TABLE [dbo].[ccc_vehicles](
    #[VehicleID] [int] NOT NULL,
    #[Vehicle] [nchar](20) NOT NULL
    #)""")

    #CURSOR.execute("""
    #CREATE TABLE[dbo].[ccc_stops](
    #[RouteStopID] [int] NOT NULL,
    #[RouteStop] [nchar](50) NOT NULL,
    #[Latitude] [float] NOT NULL,
    #[Logitude] [float] NOT NULL
    #)""")

    for i in range(numofdays):

        end = datetime(year, month, day, 23, 59, 59, 000000) - timedelta(days=i)
        start = datetime(year, month, day, 00, 00, 00, 000000) - timedelta(days=i)
        print("Processing {}".format(start.strftime("%Y-%m-%d %H:%M:%S")))

        ridership_data = shuttle.get_ridership_data(start_date=start.strftime("%Y-%m-%d %H:%M:%S"),
                                                    end_date=end.strftime("%Y-%m-%d %H:%M:%S"))

        ridership = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: list([0, 0]))))

        data = []
        routes = []
        vehicles = []
        stops = []

        for stop_data in ridership_data:
            if stop_data['RouteStopID'] == 0:
                continue
            # clean the route name
            if 'purple' in stop_data['Route'].lower():
                route = 'purple'
            elif 'green' in stop_data['Route'].lower():
                route = 'green'
            elif 'orange' in stop_data['Route'].lower():
                route = 'orange'
            elif 'banner' in stop_data['Route'].lower():
                route = 'banner'

            if 'Rear' in stop_data['Counter']:
                frontdoor = False
            else:
                frontdoor=True

            clienttime = datetime.strptime(stop_data['ClientTime'], '%m/%d/%Y %I:%M:%S %p')
            data.append((
                stop_data['ClientTime'],
                stop_data['VehicleID'],
                stop_data['RouteStopID'],
                stop_data['RouteID'],
                clienttime.hour,
                clienttime.weekday(),
                stop_data['Entries'],
                stop_data['Exits'],
                int(frontdoor)
                ))

            routes.append((
                stop_data['RouteID'],
                stop_data['Route']
                ))

            stops.append((
                stop_data['RouteStopID'],
                stop_data['RouteStop'],
                stop_data['Lattitude'],
                stop_data['Longitude']
                ))

            vehicles.append((
                stop_data['VehicleID'],
                stop_data['Vehicle']
                ))

        if not data:
            continue

        CURSOR.executemany("""
        MERGE ccc_aggregate_ridership_ex USING (
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ) AS vals ([ClientTime], [VehicleID], [RouteStopID], [RouteID], [Hour], [Weekday], [Boardings], [Alightings], [Frontdoor])
        ON (ccc_aggregate_ridership_ex.[ClientTime] = vals.[ClientTime] AND
            ccc_aggregate_ridership_ex.[VehicleID] = vals.[VehicleID] AND
            ccc_aggregate_ridership_ex.[RouteStopID] = vals.[RouteStopID] AND
            ccc_aggregate_ridership_ex.[RouteID] = vals.[RouteID])
        WHEN MATCHED THEN
            UPDATE SET
            [Boardings] = vals.[Boardings],
            [Alightings] = vals.[Alightings]
        WHEN NOT MATCHED THEN
            INSERT ([ClientTime], [VehicleID], [RouteStopID], [RouteID], [Hour], [Weekday], [Boardings], [Alightings], [Frontdoor])
            VALUES (vals.[ClientTime], vals.[VehicleID], vals.[RouteStopID], vals.[RouteID], vals.[Hour], vals.[Weekday],
            vals.[Boardings], vals.[Alightings], vals.[Frontdoor]);
        """, data)
        CURSOR.commit()

        CURSOR.executemany("""
        MERGE ccc_routes USING (
        VALUES
            (?, ?)
        ) AS vals ([RouteID], [RouteName])
        ON (ccc_routes.[RouteID] = vals.[RouteID])
        WHEN MATCHED THEN
            UPDATE SET
            [RouteName] = vals.[RouteName]
        WHEN NOT MATCHED THEN
            INSERT ([RouteID], [RouteName])
            VALUES (vals.[RouteID], vals.[RouteName]);
        """, routes)
        CURSOR.commit()

        CURSOR.executemany("""
        MERGE ccc_vehicles USING (
        VALUES
            (?, ?)
        ) AS vals ([VehicleID], [Vehicle])
        ON (ccc_vehicles.[VehicleID] = vals.[VehicleID])
        WHEN MATCHED THEN
            UPDATE SET
            [Vehicle] = vals.[Vehicle]
        WHEN NOT MATCHED THEN
            INSERT ([VehicleID], [Vehicle])
            VALUES (vals.[VehicleID], vals.[Vehicle]);
        """, vehicles)
        CURSOR.commit()

        CURSOR.executemany("""
        MERGE ccc_stops USING (
        VALUES
            (?, ?, ?, ?)
        ) AS vals ([RouteStopID], [RouteStop], [Latitude], [Longitude])
        ON (ccc_stops.[RouteStopID] = vals.[RouteStopID])
        WHEN MATCHED THEN
            UPDATE SET
            [RouteStop] = vals.[RouteStop],
            [Latitude] = vals.[Latitude],
            [Longitude] = vals.[Longitude]
        WHEN NOT MATCHED THEN
            INSERT ([RouteStopID], [RouteStop], [Latitude], [Longitude])
            VALUES (vals.[RouteStopID], vals.[RouteStop], vals.[Latitude], vals.[Longitude]);
        """, stops)

        CURSOR.commit()


def start_from_cmdline():
    """
    Parse the args and start
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
