"""Main driver for the transitstat scripts"""
import argparse
import logging
import os
import tempfile
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

from src.transitstat.circulator.realtime import process_vehicles
from src.transitstat.circulator.reports import update_database
from src.transitstat.circulator.import_gtfs import insert_stop_times, insert_trips, insert_calendar, insert_routes, \
    insert_stops
from src.transitstat.circulator.import_ridership import DataImporter

start_date = date.today() - timedelta(days=7)
parser = argparse.ArgumentParser(description="Driver for the transitstat scripts")
subparsers = parser.add_subparsers(dest='subparser_name', help='sub-command help')

parser_realtime = subparsers.add_parser('realtime', help='Pulls realtime data about the bus locations')

parser_otp = subparsers.add_parser('otp', help='Pulls the On Time Percentage report from RideSystems')
parser_otp.add_argument('-v', '--verbose', help='Debug logging level')
parser_otp.add_argument('-m', '--month', type=int, default=start_date.month,
                        help=('Optional: Month of date we should start searching on (IE: 10 for Oct). Default: {}'
                              .format(start_date)))
parser_otp.add_argument('-d', '--day', type=int, default=start_date.day,
                        help=('Optional: Day of date we should start searching on (IE: 5). Default: {}'
                              .format(start_date)))
parser_otp.add_argument('-y', '--year', type=int, default=start_date.year,
                        help=('Optional: Four digit year we should start searching on (IE: 2020). Default: {}'
                              .format(start_date)))
parser_otp.add_argument('-n', '--numofdays', default=7, type=int,
                        help='Optional: Number of days to search, including the start date. Default: 7 days')

parser_gtfs = subparsers.add_parser('gtfs', help='Updates the database with a GTFS file from RideSystems')
parser_gtfs.add_argument('-f', '--file', required=True, help='Zip file to import')
parser_gtfs.add_argument('-r', '--recreate', action='store_true', help='Drop and recreate database tables')

parser_import = subparsers.add_parser('import', help='Imports ridership data from a standard XLSX file')
parser_import.add_argument('-f', '--file', help='File to import')
parser_import.add_argument('-d', '--dir', help='Directory to process that contains XLSX files with ridership data')
parser_import.add_argument('-c', '--conn_str', help='Database connection string',
                           default='mssql+pyodbc://balt-sql311-prd/DOT_DATA?driver=ODBC Driver 17 for SQL Server')

args = parser.parse_args()

if args.subparser_name == 'realtime':
    process_vehicles()

if args.subparser_name == 'otp':
    start = datetime(args.year, args.month, args.day)
    end = start + timedelta(days=args.numofdays)

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    update_database(start, end)

if args.subparser_name == 'gtfs':
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(args.file, 'r') as zip_ref:
            zip_ref.extractall(tmpdir)
            insert_stop_times(os.path.join(tmpdir, 'stop_times.txt'), args.recreate)
            insert_trips(os.path.join(tmpdir, 'trips.txt'), args.recreate)
            insert_calendar(os.path.join(tmpdir, 'calendar.txt'), args.recreate)
            insert_routes(os.path.join(tmpdir, 'routes.txt'), args.recreate)
            insert_stops(os.path.join(tmpdir, 'stops.txt'), args.recreate)

            logging.info("Starting the harbor connector spreadsheet importer")

if args.subparser_name == 'import':
    di = DataImporter(args.conn_str)
    if args.file:
        di.import_ridership(file=Path(args.file))

    if args.dir:
        di.import_ridership(directory=Path(args.dir))
