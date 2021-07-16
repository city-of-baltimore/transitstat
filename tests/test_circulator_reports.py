"""Test suites for circulator.circulator_reports"""
from datetime import date

from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.orm import Session  # type: ignore

from transitstat.circulator.reports import get_otp
from transitstat.circulator.schema import CirculatorArrival


def test_get_otp(conn_str):
    """Test for get_otp"""
    get_otp(date(2021, 6, 1), date(2021, 6, 1), conn_str=conn_str)
    engine = create_engine(conn_str, echo=True, future=True)

    with Session(bind=engine, future=True) as session:
        ret = session.query(CirculatorArrival)
        assert ret.count() > 100


def test_get_otp_force(conn_str):
    """Test for get_otp"""
    get_otp(date(2021, 5, 1), date(2021, 5, 1), conn_str=conn_str, force=True)
    engine = create_engine(conn_str, echo=True, future=True)

    with Session(bind=engine, future=True) as session:
        ret = session.query(CirculatorArrival)
        assert ret.count() > 100


def test_get_otp_no_force(conn_str):
    """Test for get_otp"""
    get_otp(date(2021, 5, 1), date(2021, 5, 1), conn_str=conn_str)
    engine = create_engine(conn_str, echo=True, future=True)

    with Session(bind=engine, future=True) as session:
        ret = session.query(CirculatorArrival)
        assert ret.count() == 4
