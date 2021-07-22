"""Main driver for the transitstat scripts"""
import argparse
import os
import sys
import tempfile
import zipfile
from datetime import date, timedelta
from pathlib import Path

from loguru import logger

from src.transitstat.circulator.import_gtfs import insert_stop_times, insert_trips, insert_calendar, insert_routes, \
    insert_stops
from src.transitstat.circulator.import_ridership import DataImporter
from src.transitstat.circulator.reports import RidesystemReports

parser = argparse.ArgumentParser(description="Driver for the transitstat scripts")
parser.add_argument('-v', '--verbose', action='store_true', help='Increased logging level')
parser.add_argument('-d', '--debug', action='store_true', help='Print debug statements')
parser.add_argument('-c', '--conn_str', help='Database connection string',
                    default='mssql+pyodbc://balt-sql311-prd/DOT_DATA?driver=ODBC Driver 17 for SQL Server')

subparsers = parser.add_subparsers(dest='subparser_name', help='sub-command help')

parser_otp = subparsers.add_parser('otp', help='Pulls the On Time Percentage report from RideSystems')
parser_otp.add_argument('-m', '--month', type=int,
                        help='Optional: Month of date we should start searching on (IE: 10 for Oct).')
parser_otp.add_argument('-d', '--day', type=int,
                        help='Optional: Day of date we should start searching on (IE: 5).')
parser_otp.add_argument('-y', '--year', type=int,
                        help='Optional: Four digit year we should start searching on (IE: 2020).')
parser_otp.add_argument('-n', '--numofdays', type=int,
                        help='Optional: Number of days to search, including the start date.')
parser_otp.add_argument('-o', '--force', action='store_true',
                        help='By default, it skips dates that already have data. This flag regenerates the date range.')

parser_gtfs = subparsers.add_parser('gtfs', help='Updates the database with a GTFS file from RideSystems')
parser_gtfs.add_argument('-f', '--file', required=True, help='Zip file to import')
parser_gtfs.add_argument('-r', '--recreate', action='store_true', help='Drop and recreate database tables')

parser_import = subparsers.add_parser('import', help='Imports ridership data from a standard XLSX file')
parser_import.add_argument('-f', '--file', help='File to import')
parser_import.add_argument('-d', '--dir', help='Directory to process that contains XLSX files with ridership data')
parser_import.add_argument('-c', '--conn_str', help='Database connection string',
                           default='mssql+pyodbc://balt-sql311-prd/DOT_DATA?driver=ODBC Driver 17 for SQL Server')

args = parser.parse_args()

# Setup logging
LOG_LEVEL = 'WARNING'
if args.debug:
    LOG_LEVEL = 'DEBUG'
elif args.verbose:
    LOG_LEVEL = 'INFO'

handlers = [
    {'sink': sys.stdout, 'format': '{time} - {message}', 'colorize': True, 'backtrace': True, 'diagnose': True,
     'level': LOG_LEVEL},
    {'sink': os.path.join('logs', 'file-{time}.log'), 'serialize': True, 'backtrace': True,
     'diagnose': True, 'rotation': '1 week', 'retention': '3 months', 'compression': 'zip', 'level': LOG_LEVEL},
]

logger.configure(handlers=handlers)

# On time percentage
if args.subparser_name == 'otp':
    rs = RidesystemReports(args.conn_str)
    if args.year and args.month and args.day and args.numofdays:
        start_date = date(args.year, args.month, args.day)
        end_date = start_date + timedelta(days=args.numofdays)
        rs.get_otp(start_date, end_date)
    elif args.year or args.month or args.day or args.numofdays:
        logger.critical('If you specify a year/month/day/numofdays, then you must specify them all.')
    else:
        start_date = date(2020, 1, 1)
        end_date = date.today() - timedelta(days=1)
        rs.get_otp(start_date, end_date)

# GTFS parsing
if args.subparser_name == 'gtfs':
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(args.file, 'r') as zip_ref:
            zip_ref.extractall(tmpdir)
            insert_stop_times(os.path.join(tmpdir, 'stop_times.txt'), args.recreate)
            insert_trips(os.path.join(tmpdir, 'trips.txt'), args.recreate)
            insert_calendar(os.path.join(tmpdir, 'calendar.txt'), args.recreate)
            insert_routes(os.path.join(tmpdir, 'routes.txt'), args.recreate)
            insert_stops(os.path.join(tmpdir, 'stops.txt'), args.recreate)

# Import ridership
if args.subparser_name == 'import':
    di = DataImporter(args.conn_str)
    if args.file:
        di.import_ridership(file=Path(args.file))

    if args.dir:
        di.import_ridership(directory=Path(args.dir))
