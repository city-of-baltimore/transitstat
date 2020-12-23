"""Main driver for the transitstat scripts"""
import argparse
from src.transitstat.connector import parse_sheets, insert_into_db

parser = argparse.ArgumentParser(description="Driver for the Harbor Connector scripts")

parser.add_argument('-p', '--path',
                    help='File or directory to import. If directory is provided, then all files will be processed')

args = parser.parse_args()

ridership = parse_sheets(args.path)
insert_into_db(ridership)
