"""Test suite for transitstat.circulator.otp_reports"""
from pathlib import Path

from transitstat.circulator.otp_reports import read_operator_report


def test_read_operator_report(conn_str):
    """Test for read_operator_report"""
    read_operator_report(conn_str, Path('tests') / "data" / 'March 2022 - Dispatch Report.xlsx')
    read_operator_report(conn_str, Path('tests') / "data" / 'April 2022 Dispatch Report.xlsx')
    read_operator_report(conn_str, Path('tests') / "data" / 'May 2022, DISPATCH REPORT.xlsx')
    read_operator_report(conn_str, Path('tests') / "data" / 'June 2022 - Dispatch Report.xlsx')
    read_operator_report(conn_str, Path('tests') / "data" / 'July 2022 - Dispatch Report.xlsx')
    read_operator_report(conn_str, Path('tests') / "data" / 'August 2022-Dispatch Report.xlsx')
    read_operator_report(conn_str, Path('tests') / "data" / 'Sept 2022 - Dispatch Report.xlsx')
