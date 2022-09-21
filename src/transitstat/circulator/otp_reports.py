"""Reads the operator reports that RMA Generates"""
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd  # type: ignore
from sqlalchemy import create_engine  # type: ignore

from transitstat.args import setup_logging, setup_parser
from .schema import Base


def read_operator_report(conn_str, path: Path):
    """
    Reads the operator reports sent by RMA
    """
    engine = create_engine(conn_str, echo=True, future=True)
    with engine.begin() as connection:
        Base.metadata.create_all(connection)

    # Filter non-date sheets
    excel_file = pd.ExcelFile(path)
    valid_sheets = [i for i in excel_file.sheet_names if re.match(r'\d{1,2}\.\d{1,2}\.\d{2,4}', i)]
    for sheet_name in valid_sheets:
        df = excel_file.parse(sheet_name)

        # Drop rows where the first column doesn't have numbers, or operator name is empty
        df = df.loc[df.iloc[:, 0].str.contains(r'\d{3,4}', na=False)]
        df = df.loc[df.iloc[:, 2].str.contains(r'.+', na=False)]
        column_names = ['Bus', 'Block', 'Operator', 'Nextel', 'Clock In/Temp', 'Start Time', 'Notes', 'Relief Vehicle',
                        'Relief Location/Time', 'End Time', 'Clock Out']
        extra_cols = len(df.columns) - len(column_names)
        if extra_cols > 0:
            column_names += ['NA'] * extra_cols

        df.columns = column_names

        df['Vehicle'] = 'Bus ' + df['Bus'].str.upper().str.extract(r'CC\d{4}\((\d{2})\)')
        df['Route'] = df['Block'].str.upper().str.extract(r'(ORANGE|PURPLE|BANNER|GREEN)')
        df['Date'] = datetime.strptime(sheet_name.strip(), '%m.%d.%y')
        df['Time_of_day'] = df['Block'].str.extract('(PM)').fillna('AM')

        block_df = df['Block'].str.upper().str.extract(r'^(P|O|B|G)\w*[ ]?(\d)')
        df['Block'] = block_df[0] + block_df[1]
        df['Bus'] = 'CC' + df['Bus'].str.upper().str.extract(r'(\d{3,4})')

        for i in ['Nextel', 'NA', 'Clock In/Temp', 'Relief Location/Time', 'Clock Out', 'Start Time',
                  'Notes', 'Relief Vehicle', 'End Time']:
            df.drop(i, axis=1, inplace=True)

        df.to_sql('ccc_operators', con=engine, if_exists='append', index=False)


def parse_args(args):
    """Handles argument parsing"""
    parser = setup_parser('Parses the operator reports')

    parser.add_argument('-f', '--file', required=True, help='Excel file to import')

    return parser.parse_args(args)


if __name__ == '__main__':
    parsed_args = parse_args(sys.argv[1:])
    setup_logging(parsed_args.debug, parsed_args.verbose)

    read_operator_report(parsed_args.conn_str, parsed_args.file)
