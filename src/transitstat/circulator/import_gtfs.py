"""
Imports the GTFS.zip file into the database
"""
import csv
import os
import sys
import tempfile
import zipfile
from typing import Union

import pyodbc  # type: ignore
from ridesystems.api import API

from transitstat.args import setup_logging, setup_parser
from .creds import RIDESYSTEMS_API_KEY


class ImportGtfs:
    """Handles importing the GTFS files into the database"""

    def __init__(self):
        conn = pyodbc.connect(r'mssql+pyodbc://balt-sql311-prd/DOT_DATA?driver=ODBC Driver 17 for SQL Server')
        self.cursor = conn.cursor()

    def insert_calendar(self, data_file: Union[str, bytes], recreate_table: bool = False) -> None:
        """
        service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
        :param data_file:
        :param recreate_table:
        """
        if recreate_table:
            try:
                self.cursor.execute("""DROP TABLE ccc_gtfs_calendar;""")
                self.cursor.commit()
            except pyodbc.ProgrammingError:
                print("ccc_gtfs_calendar does not exist")

            self.cursor.execute("""
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
            self.cursor.commit()
        self._insert(data_file, "ccc_gtfs_calendar")

    def insert_routes(self, data_file: Union[str, bytes], recreate_table: bool = False):
        """
        route_id,route_short_name,route_long_name,route_desc,route_type,route_color

        :param data_file: (str) Path to the file to insert into the table
        :param recreate_table: (bool) If the table exists, then drop it and recreate it
        :return: None
        """
        if recreate_table:
            try:
                self.cursor.execute("""DROP TABLE ccc_gtfs_routes;""")
                self.cursor.commit()
            except pyodbc.ProgrammingError:
                print("ccc_gtfs_routes does not exist")

            self.cursor.execute("""
                CREATE TABLE [dbo].[ccc_gtfs_routes] (
                [route_id] VARCHAR(100),
                [route_short_name] VARCHAR(50) NOT NULL,
                [route_long_name] VARCHAR(255) NOT NULL,
                [route_desc] VARCHAR(255) NOT NULL,
                [route_type] VARCHAR(2) NOT NULL,
                [route_color] VARCHAR(255),
                [route_sort_order] VARCHAR(10)
            );""")
            self.cursor.commit()
        self._insert(data_file, "ccc_gtfs_routes")

    def insert_stop_times(self, data_file: Union[str, bytes], recreate_table=False) -> None:
        """
        trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type

        :param data_file: (str) Path to the file to insert into the table
        :param recreate_table: (bool) If the table exists, then drop it and recreate it
        :return: None
        """
        if recreate_table:
            try:
                self.cursor.execute("""DROP TABLE ccc_gtfs_stop_times;""")
                self.cursor.commit()
            except pyodbc.ProgrammingError:
                print("ccc_gtfs_stop_times does not exist")

            self.cursor.execute("""
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
            self.cursor.commit()
        self._insert(data_file, "ccc_gtfs_stop_times")

    def insert_stops(self, data_file: Union[str, bytes], recreate_table: bool = False):
        """
        stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,location_type,parent_station

        :param data_file: (str) Path to the file to insert into the table
        :param recreate_table: (bool) If the table exists, then drop it and recreate it
        :return: None
        """
        if recreate_table:
            try:
                self.cursor.execute("""DROP TABLE ccc_gtfs_stops;""")
                self.cursor.commit()
            except pyodbc.ProgrammingError:
                print("ccc_gtfs_stops does not exist")

            self.cursor.execute("""
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
            self.cursor.commit()
        self._insert(data_file, "ccc_gtfs_stops")

        # Because ridesystems is awful, and can't use a single RouteStopID (or even only two, or three... there are at
        # least FOUR!!), we have to do this absolutely awful hack here to figure out what they might mean
        rs_interface = API(RIDESYSTEMS_API_KEY)
        dumb_stoplist = rs_interface.get_stops(4) + rs_interface.get_stops(10) + rs_interface.get_stops(12) + \
            rs_interface.get_stops(13)
        dumb_routestopids = {i['RouteStopID']: [i['Description'], i['Latitude'], i['Longitude'], i['RouteID']]
                             for i in dumb_stoplist}

        for dumb_routestopid, dumb_routedata in dumb_routestopids.items():
            self.cursor.execute("""
                SELECT DISTINCT ccc_gtfs_stop_times.stop_id, ccc_gtfs_stops.stop_lat, ccc_gtfs_stops.stop_lon
                FROM ccc_gtfs_stop_times
                JOIN ccc_gtfs_stops
                ON ccc_gtfs_stop_times.stop_id = ccc_gtfs_stops.stop_id
                JOIN ccc_gtfs_trips
                ON ccc_gtfs_stop_times.trip_id = ccc_gtfs_trips.trip_id
                WHERE ccc_gtfs_stops.stop_name = ? AND ccc_gtfs_trips.route_id = ?""",
                                dumb_routedata[0], dumb_routedata[3])
            dumb_stopids = self.cursor.fetchall()
            if len(dumb_stopids) > 1:
                dumb_routeid_guess = {abs(dumb_routedata[1] - float(i[1])) + abs(dumb_routedata[2] - float(i[2])): i[0]
                                      for i in dumb_stopids}

                print(f'MULTIPLES - name: {dumb_routedata[0]}'
                      f'Lat/Long: {dumb_routedata[1]}/{dumb_routedata[2]}'
                      f'Routeids: {dumb_stopids}'
                      f'Differences: {dumb_routeid_guess}'
                      f'Guessed: {dumb_routeid_guess[min(dumb_routeid_guess.keys())]}\n\n')
                dumb_stopids = dumb_routeid_guess[min(dumb_routeid_guess.keys())]

            elif len(dumb_stopids) == 1:
                dumb_stopids = dumb_stopids[0][0]
            else:
                print(f'No results for {dumb_routedata[0]}')
                continue

            self.cursor.execute("""UPDATE ccc_gtfs_stops SET dumb_stop_id = ? WHERE stop_id = ?""",
                                dumb_routestopid,
                                dumb_stopids)
            self.cursor.commit()

    def get_route_from_stop(self, stop_id: int) -> int:
        """
        Takes the GTFS stop_id value and gets the route_id
        :param stop_id: (int) The stop_id from GTFS
        :return: (int) route_id
        """
        self.cursor.execute("""
            SELECT ccc_gtfs_stop_times.stop_id, ccc_gtfs_stop_times.trip_id, ccc_gtfs_trips.trip_id, ccc_gtfs_trips.route_id
            FROM ccc_gtfs_stop_times
            JOIN ccc_gtfs_trips
            ON ccc_gtfs_stop_times.trip_id = ccc_gtfs_trips.trip_id
            WHERE ccc_gtfs_stop_times.stop_id = ?""", stop_id)
        res = self.cursor.fetchall()
        return res[0][3]

    def insert_trips(self, data_file: Union[str, bytes], recreate_table: bool = False) -> None:
        """
        route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id

        :param data_file: Path to the file to insert into the table
        :param recreate_table: (bool) If the table exists, then drop it and recreate it
        :return: None
        """
        if recreate_table:
            try:
                self.cursor.execute("""DROP TABLE ccc_gtfs_trips;""")
                self.cursor.commit()
            except pyodbc.ProgrammingError:
                print("ccc_gtfs_trips does not exist")

            self.cursor.execute("""
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
            self.cursor.commit()

        self._insert(data_file, "ccc_gtfs_trips")

    def _insert(self, data_file: Union[str, bytes], table_name: str) -> None:
        """
        Insert a comma separated value file into a table. Expects the first row to be the columns.
        :param data_file: Path to file to read
        :param table_name: Table to insert data into. Should have columns that match the first row of data_file
        :return:
        """
        with open(data_file, encoding='utf-8') as csv_file:
            reader = csv.reader(csv_file)
            columns = next(reader)
            query = f"insert into {table_name}({','.join(columns)}) values ({','.join('?' * len(columns))})"
            self.cursor.executemany(query, reader)
            self.cursor.commit()


def parse_args(args):
    """Handles argument parsing"""
    parser = setup_parser('Updates the database with a GTFS file from RideSystems')

    parser.add_argument('-f', '--file', required=True, help='Zip file to import')
    parser.add_argument('-r', '--recreate', action='store_true', help='Drop and recreate database tables')

    return parser.parse_args(args)


if __name__ == '__main__':
    parsed_args = parse_args(sys.argv[1:])
    setup_logging(parsed_args.debug, parsed_args.verbose)

    gtfs = ImportGtfs()

    # GTFS parsing
    if parsed_args.subparser_name == 'gtfs':
        with tempfile.TemporaryDirectory() as tmpdir:
            with zipfile.ZipFile(parsed_args.file, 'r') as zip_ref:
                zip_ref.extractall(tmpdir)
                gtfs.insert_stop_times(os.path.join(tmpdir, 'stop_times.txt'), parsed_args.recreate)
                gtfs.insert_trips(os.path.join(tmpdir, 'trips.txt'), parsed_args.recreate)
                gtfs.insert_calendar(os.path.join(tmpdir, 'calendar.txt'), parsed_args.recreate)
                gtfs.insert_routes(os.path.join(tmpdir, 'routes.txt'), parsed_args.recreate)
                gtfs.insert_stops(os.path.join(tmpdir, 'stops.txt'), parsed_args.recreate)
