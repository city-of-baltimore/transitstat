"""
Imports the GTFS.zip file into the database
"""
import argparse
import csv
import os
import tempfile
import zipfile

import pyodbc  # type: ignore
from .shuttle import get_stops


CONN = pyodbc.connect(r'Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
CURSOR = CONN.cursor()


def insert_calendar(data_file, recreate_table=False):
    """
    service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date

    :param data_file:
    :param recreate_table:
    :return:
    """
    if recreate_table:
        try:
            CURSOR.execute("""DROP TABLE ccc_gtfs_calendar;""")
            CURSOR.commit()
        except pyodbc.ProgrammingError:
            print("ccc_gtfs_calendar does not exist")

        CURSOR.execute("""
            CREATE TABLE [dbo].[ccc_gtfs_calendar] (
            [service_id] VARCHAR(255) NOT NULL,
            [monday] BIT NOT NULL,
            [tuesday] BIT NOT NULL,
            [wednesday] BIT NOT NULL,
            [thursday] BIT NOT NULL,
            [friday] BIT NOT NULL,
            [saturday] BIT NOT NULL,
            [sunday] BIT NOT NULL,
            [start_date] VARCHAR(8) NOT NULL,
            [end_date] VARCHAR(8) NOT NULL
        );""")
        CURSOR.commit()
    _insert(data_file, "ccc_gtfs_calendar")


def insert_routes(data_file, recreate_table=False):
    """
    route_id,route_short_name,route_long_name,route_desc,route_type,route_color

    :param data_file: (str) Path to the file to insert into the table
    :param recreate_table: (bool) If the table exists, then drop it and recreate it
    :return: None
    """
    if recreate_table:
        try:
            CURSOR.execute("""DROP TABLE ccc_gtfs_routes;""")
            CURSOR.commit()
        except pyodbc.ProgrammingError:
            print("ccc_gtfs_routes does not exist")

        CURSOR.execute("""
            CREATE TABLE [dbo].[ccc_gtfs_routes] (
            [route_id] VARCHAR(100),
            [route_short_name] VARCHAR(50) NOT NULL,
            [route_long_name] VARCHAR(255) NOT NULL,
            [route_desc] VARCHAR(255) NOT NULL,
            [route_type] VARCHAR(2) NOT NULL,
            [route_color] VARCHAR(255)
        );""")
        CURSOR.commit()
    _insert(data_file, "ccc_gtfs_routes")


def insert_stop_times(data_file, recreate_table=False):
    """
    trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type

    :param data_file: (str) Path to the file to insert into the table
    :param recreate_table: (bool) If the table exists, then drop it and recreate it
    :return: None
    """
    if recreate_table:
        try:
            CURSOR.execute("""DROP TABLE ccc_gtfs_stop_times;""")
            CURSOR.commit()
        except pyodbc.ProgrammingError:
            print("ccc_gtfs_stop_times does not exist")

        CURSOR.execute("""
            CREATE TABLE [dbo].[ccc_gtfs_stop_times] (
            [trip_id] VARCHAR(100) NOT NULL,
            [arrival_time] VARCHAR(8) NOT NULL,
            [departure_time] VARCHAR(8) NOT NULL,
            [stop_id] VARCHAR(100) NOT NULL,
            [stop_sequence] VARCHAR(100) NOT NULL,
            [stop_headsign] VARCHAR(50),
            [pickup_type] VARCHAR(2),
            [drop_off_type] VARCHAR(2)
        );""")
        CURSOR.commit()
    _insert(data_file, "ccc_gtfs_stop_times")


def insert_stops(data_file, recreate_table=False):
    """
    stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,location_type,parent_station

    :param data_file: (str) Path to the file to insert into the table
    :param recreate_table: (bool) If the table exists, then drop it and recreate it
    :return: None
    """
    if recreate_table:
        try:
            CURSOR.execute("""DROP TABLE ccc_gtfs_stops;""")
            CURSOR.commit()
        except pyodbc.ProgrammingError:
            print("ccc_gtfs_stops does not exist")

        CURSOR.execute("""
            CREATE TABLE [dbo].[ccc_gtfs_stops] (
            [stop_id] VARCHAR(255),
            [dumb_stop_id] VARCHAR(255),
            [stop_code] VARCHAR(50),
            [stop_name] VARCHAR(255) NOT NULL,
            [stop_desc] VARCHAR(255),
            [stop_lat] DECIMAL(10,6) NOT NULL,
            [stop_lon] DECIMAL(10,6) NOT NULL,
            [stop_url] VARCHAR(255),
            [location_type] VARCHAR(2),
            [parent_station] VARCHAR(100)
        );""")
        CURSOR.commit()
    _insert(data_file, "ccc_gtfs_stops")

    # Because ridesystems is awful, and can't use a single RouteStopID (or even only two, or three... there are at least
    # FOUR!!), we have to do this absolutely awful hack here to figure out what they might mean
    dumb_stoplist = get_stops('4') + get_stops('10') + get_stops('12') + get_stops('13')
    dumb_routestopids = {i['RouteStopID']: [i['Description'], i['Latitude'], i['Longitude'], i['RouteID']]
                         for i in dumb_stoplist}

    for dumb_routestopid, dumb_routedata in dumb_routestopids.items():
        CURSOR.execute("""
            SELECT DISTINCT ccc_gtfs_stop_times.stop_id, ccc_gtfs_stops.stop_lat, ccc_gtfs_stops.stop_lon
            FROM ccc_gtfs_stop_times
            JOIN ccc_gtfs_stops
            ON ccc_gtfs_stop_times.stop_id = ccc_gtfs_stops.stop_id
            JOIN ccc_gtfs_trips
            ON ccc_gtfs_stop_times.trip_id = ccc_gtfs_trips.trip_id
            WHERE ccc_gtfs_stops.stop_name = ? AND ccc_gtfs_trips.route_id = ?""",
                       dumb_routedata[0], dumb_routedata[3])
        dumb_stopids = CURSOR.fetchall()
        if len(dumb_stopids) > 1:
            dumb_routeid_guess = {abs(dumb_routedata[1] - float(i[1])) + abs(dumb_routedata[2] - float(i[2])): i[0]
                                  for i in dumb_stopids}

            print("MULTIPLES - name: {}\nLat/Long: {}/{}\nRouteids: {}\nDifferences: {}\nGuessed: {}\n\n".format(
                dumb_routedata[0],
                dumb_routedata[1],
                dumb_routedata[2],
                dumb_stopids,
                dumb_routeid_guess,
                dumb_routeid_guess[min(dumb_routeid_guess.keys())]))
            dumb_stopids = dumb_routeid_guess[min(dumb_routeid_guess.keys())]

        elif len(dumb_stopids) == 1:
            dumb_stopids = dumb_stopids[0][0]
        else:
            print("No results for {}".format(dumb_routedata[0]))
            continue

        CURSOR.execute("""UPDATE ccc_gtfs_stops SET dumb_stop_id = ? WHERE stop_id = ?""",
                       dumb_routestopid,
                       dumb_stopids)
        CURSOR.commit()


def get_route_from_stop(stop_id):
    """
    Takes the GTFS stop_id value and gets the route_id
    :param stop_id: (int) The stop_id from GTFS
    :return: (int) route_id
    """
    CURSOR.execute("""
        SELECT ccc_gtfs_stop_times.stop_id, ccc_gtfs_stop_times.trip_id, ccc_gtfs_trips.trip_id, ccc_gtfs_trips.route_id
        FROM ccc_gtfs_stop_times
        JOIN ccc_gtfs_trips
        ON ccc_gtfs_stop_times.trip_id = ccc_gtfs_trips.trip_id
        WHERE ccc_gtfs_stop_times.stop_id = ?""", stop_id)
    res = CURSOR.fetchall()
    return res[0][3]


def insert_trips(data_file, recreate_table=False):
    """
    route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id

    :param data_file: (str) Path to the file to insert into the table
    :param recreate_table: (bool) If the table exists, then drop it and recreate it
    :return: None
    """
    if recreate_table:
        try:
            CURSOR.execute("""DROP TABLE ccc_gtfs_trips;""")
            CURSOR.commit()
        except pyodbc.ProgrammingError:
            print("ccc_gtfs_trips does not exist")

        CURSOR.execute("""
            CREATE TABLE [dbo].[ccc_gtfs_trips] (
            [route_id] VARCHAR(100) NOT NULL,
            [service_id] VARCHAR(100) NOT NULL,
            [trip_id] VARCHAR(255),
            [trip_headsign] VARCHAR(255),
            [trip_short_name] VARCHAR(255),
            [direction_id] BIT,
            /*#0 for one direction, 1 for another.*/
            [block_id] VARCHAR(40),
            [shape_id] VARCHAR(40)
        );""")
        CURSOR.commit()

    _insert(data_file, "ccc_gtfs_trips")


def _insert(data_file, table_name):
    """

    :param data_file:
    :param table_name:
    :return:
    """
    with open(data_file) as csv_file:
        reader = csv.reader(csv_file)
        columns = next(reader)
        query = 'insert into {0}({1}) values ({2})'.format(
            table_name, ','.join(columns), ','.join('?' * len(columns)))
        CURSOR.executemany(query, reader)
        CURSOR.commit()


def start_from_cmd_line():
    """
    Parse the args and start
    """
    parser = argparse.ArgumentParser(description='GTFS data importer')
    parser.add_argument('-f', '--file', required=True, help='Zip file to import')
    parser.add_argument('-r', '--recreate', action='store_true', help='Drop and recreate database tables')

    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(args.file, 'r') as zip_ref:
            zip_ref.extractall(tmpdir)
            insert_stop_times(os.path.join(tmpdir, 'stop_times.txt'), args.recreate)
            insert_trips(os.path.join(tmpdir, 'trips.txt'), args.recreate)
            insert_calendar(os.path.join(tmpdir, 'calendar.txt'), args.recreate)
            insert_routes(os.path.join(tmpdir, 'routes.txt'), args.recreate)
            insert_stops(os.path.join(tmpdir, 'stops.txt'), args.recreate)


if __name__ == '__main__':
    start_from_cmd_line()
