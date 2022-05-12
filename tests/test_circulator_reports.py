"""Test suites for circulator.circulator_reports"""
from datetime import date, time, datetime
from unittest.mock import patch

import pandas as pd
from sqlalchemy.orm import Session  # type: ignore

from transitstat.circulator.schema import CirculatorArrival, CirculatorBusRuntimes, CirculatorRidership
from transitstat.circulator.reports import parse_args, RidesystemReports
from .factories import ArrivalFactory



def test_factory(fake_session):
    """Test for factory"""
    ArrivalFactory()
    ArrivalFactory(scheduled_arrival_time=time(12, 0))

    assert fake_session.query(CirculatorArrival).count() == 2

@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_otp_no_force(mocked_rs_cls, conn_str):
    """Test for get_otp"""
    test_df = {
        'date': [date(2022, 1, 1), date(2022, 1, 1), date(2022, 1, 1), date(2022, 1, 1)],
        'route': ['xx', 'xx', 'xx', 'xx'],
        'stop': ['xx', 'xx', 'xx', 'xx'],
        'blockid': ['xx', 'xx', 'xx', 'xx'],
        'actualarrivaltime': [time(), time(), time(), time()],
        'actualdeparturetime': [time(), time(), time(), time()],
        'scheduledarrivaltime': [time(), time(1), time(2), time(3)],
        'scheduleddeparturetime': [time(), time(), time(), time()],
        'vehicle': ['xx', 'xx', 'xx', 'xx'],
        'ontimestatus': ['xx', 'xx', 'xx', 'xx']
        }

    inst = RidesystemReports(conn_str)
    inst.rs_cls._login.return_value = "Login successful :)"
    inst.rs_cls.get_otp.return_value = pd.DataFrame(test_df)

    inst.get_otp(date(2022, 1, 1), date(2022, 1, 1), hours='12')
    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorArrival)
        assert ret.count() == 8

@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_otp_force(mocked_rs_cls, conn_str):
    """Test for get_otp"""
    test_df = {
        'date': [date(2021, 5, 1), date(2021, 5, 2), date(2021, 5, 3), date(2021, 5, 4)],
        'route': ['xx', 'xx', 'xx', 'xx'],
        'stop': ['xx', 'xx', 'xx', 'xx'],
        'blockid': ['xx', 'xx', 'xx', 'xx'],
        'actualarrivaltime': [time(), time(), time(), time()],
        'actualdeparturetime': [time(), time(), time(), time()],
        'scheduledarrivaltime': [time(), time(), time(), time()],
        'scheduleddeparturetime': [time(), time(), time(), time()],
        'vehicle': ['1', '2', '3', '4'],
        'ontimestatus': ['xx', 'xx', 'xx', 'xx']
        }

    inst = RidesystemReports(conn_str)
    inst.rs_cls._login.return_value = "Login successful :)"
    inst.rs_cls.get_otp.return_value = pd.DataFrame(test_df)

    inst.get_otp(date(2021, 5, 1), date(2021, 5, 1), force=True, hours='12')
    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorArrival)
        assert ret.count() == 4

@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_vehicle_assignments_no_force(mocked_rs_cls, conn_str):
    """Test get_vehicle_assignments"""
    test_df = {
        'vehicle': ['1', '2', '3'],
        'route':['xx', 'xx', 'xx'],
        'start_time': [datetime(2022, 1, 1, 12, 0), datetime(2022, 1, 1, 12, 15), datetime(2022, 1, 1, 12, 30)],
        'end_time': [datetime(2022, 1, 1, 12, 0), datetime(2022, 1, 1, 12, 15), datetime(2022, 1, 1, 12, 30)]
        }

    inst = RidesystemReports(conn_str)
    inst.rs_cls._login.return_value = "Login successful :)"
    inst.rs_cls.get_runtimes.return_value = pd.DataFrame(test_df)

    inst.get_vehicle_assignments(date(2022, 1, 1), date(2022, 1, 1))
    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorBusRuntimes)
        assert ret.count() == 7

@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_vehicle_assignments_force(mocked_rs_cls, conn_str):
    """Test get_vehicle_assignments with the force option on existing data"""
    test_df = {
        'vehicle': ['Purple', 'Purple', 'Purple'],
        'route':['xx', 'xx', 'xx'],
        'start_time': [datetime(2021, 5, 1, 12, 0), datetime(2021, 5, 2, 12, 0), datetime(2021, 5, 3, 12, 0)],
        'end_time': [datetime(2022, 1, 1, 12, 0), datetime(2022, 1, 1, 12, 15), datetime(2022, 1, 1, 12, 30)]
        }

    inst = RidesystemReports(conn_str)
    inst.rs_cls._login.return_value = "Login successful :)"
    inst.rs_cls.get_runtimes.return_value = pd.DataFrame(test_df)

    inst.get_vehicle_assignments(date(2022, 1, 1), date(2022, 1, 1), force=True)
    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorBusRuntimes)
        assert ret.count() == 4

@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_ridership_no_force(mocked_rs_cls, conn_str):
    """Test get_ridership"""
    test_df = {
        'vehicle': ['Tesla Model S', 'bike', 'legs'],
        'route': ['Purple', 'Purple', 'Purple'],
        'stop': ["Webster's house", 'Brian St', 'Quilvio is cool'],
        'latitude': [123.456789, 123.456789, 123.456789],
        'longitude': [123.456789, 123.456789, 123.456789],
        'datetime': [datetime(2022, 1, 1, 12, 0), datetime(2022, 1, 1, 12, 15), datetime(2022, 1, 1, 12, 30)],
        'entries': [0, 3, 1],
        'exits': [100, 0, 0],
        }

    inst = RidesystemReports(conn_str)
    inst.rs_cls._login.return_value = "Login successful :)"
    inst.rs_cls.get_ridership.return_value = pd.DataFrame(test_df)

    inst.get_ridership(date(2022, 1, 1), date(2022, 1, 1))
    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorRidership)
        assert ret.count() == 7

@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_ridership_force(mocked_rs_cls, conn_str):
    """Test get_ridership"""
    test_df = {
        'vehicle': ['xx1', 'xx2', 'xx3'],
        'route': ['xx1', 'xx2', 'xx3'],
        'stop': ['xx1', 'xx2', 'xx3'],
        'latitude': [987.654321, 987.654321, 987.654321],
        'longitude': [987.654321, 987.654321, 987.654321],
        'datetime': [datetime(2021, 5, 1, 12, 0), datetime(2021, 5, 2, 12, 0), datetime(2021, 5, 3, 12, 0)],
        'entries': [0, 3, 1],
        'exits': [100, 0, 0],
        }

    inst = RidesystemReports(conn_str)
    inst.rs_cls._login.return_value = "Login successful :)"
    inst.rs_cls.get_ridership.return_value = pd.DataFrame(test_df)

    inst.get_ridership(date(2022, 1, 1), date(2022, 1, 1), force=True)
    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorRidership)
        assert ret.count() == 4



def test_get_dates_to_process(ridesystems_reports):
    """Test get_dates_to_process"""

    # Test with datetime
    dates = ridesystems_reports.get_dates_to_process(date(2021, 5, 1), date(2021, 5, 4),
                                                    CirculatorBusRuntimes.starttime, force=False)
    assert len(dates) == 0

    dates = ridesystems_reports.get_dates_to_process(date(2021, 5, 1), date(2021, 5, 4),
                                                    CirculatorBusRuntimes.starttime, force=True)
    assert len(dates) == 4

    # Test with date
    dates = ridesystems_reports.get_dates_to_process(date(2021, 5, 1), date(2021, 5, 4),
                                                    CirculatorArrival.date, force=False)
    assert len(dates) == 0

    dates = ridesystems_reports.get_dates_to_process(date(2021, 5, 1), date(2021, 5, 4),
                                                    CirculatorArrival.date, force=True)
    assert len(dates) == 4


def test_parse_args():
    """Test parse_args"""
    conn_str = 'conn_str'
    start_date_str = '2021-07-20'
    start_date = date(2021, 7, 20)
    end_date_str = '2021-07-21'
    end_date = date(2021, 7, 21)
    args = parse_args(['-v', '-c', conn_str, 'otp', '-s', start_date_str, '-e', end_date_str, '-f'])
    assert args.verbose
    assert not args.debug
    assert args.conn_str == conn_str
    assert args.startdate == start_date
    assert args.enddate == end_date
    assert args.force

    args = parse_args(['-vv', '-c', conn_str, 'runtimes', '-s', start_date_str, '-e', end_date_str])
    assert not args.verbose
    assert args.debug
    assert args.conn_str == conn_str
    assert args.startdate == start_date
    assert args.enddate == end_date
    assert not args.force

    args = parse_args(['-vv', '-c', conn_str, 'ridership', '-s', start_date_str, '-e', end_date_str])
    assert not args.verbose
    assert args.debug
    assert args.conn_str == conn_str
    assert args.startdate == start_date
    assert args.enddate == end_date
    assert not args.force
