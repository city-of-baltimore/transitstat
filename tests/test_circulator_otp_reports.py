"""Test suite for transitstat.circulator.otp_reports"""
from pathlib import Path

from transitstat.circulator.otp_reports import read_operator_report


def test_read_operator_report(conn_str):
    """Test for read_operator_report"""
    read_operator_report(conn_str, Path('tests') / "data" / 'March 2022 - Dispatch Report.xlsx')
