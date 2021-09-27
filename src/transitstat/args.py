"""Setup logging"""
import argparse
import os
import sys

from loguru import logger


def setup_parser(help_str="Driver for the transitstat scripts"):
    """Factory that creates the base argument parser"""
    parser = argparse.ArgumentParser(description=help_str)
    parser.add_argument('-v', '--verbose', action='store_true', help='Increased logging level')
    parser.add_argument('-vv', '--debug', action='store_true', help='Print debug statements')
    parser.add_argument('-c', '--conn_str', help='Database connection string',
                        default='mssql+pyodbc://balt-sql311-prd/DOT_DATA?driver=ODBC Driver 17 for SQL Server')

    return parser


def setup_logging(debug=False, verbose=False):
    """
    Configures the logging level, and sets up file based logging

    :param debug: If true, the Debug logging level is used, and verbose is ignored
    :param verbose: If true and debug is false, then the info log level is used
    """
    # Setup logging
    log_level = 'WARNING'
    if debug:
        log_level = 'DEBUG'
    elif verbose:
        log_level = 'INFO'

    handlers = [
        {'sink': sys.stdout, 'format': '{time} - {message}', 'colorize': True, 'backtrace': True, 'diagnose': True,
         'level': log_level},
        {'sink': os.path.join('logs', 'file-{time}.log'), 'serialize': True, 'backtrace': True,
         'diagnose': True, 'rotation': '1 week', 'retention': '3 months', 'compression': 'zip', 'level': log_level},
    ]

    logger.configure(handlers=handlers)
