"""Test suites for circulator.circulator_reports"""
from datetime import date, time, datetime
import pandas as pd
from unittest.mock import patch

from sqlalchemy.orm import Session  # type: ignore

from transitstat.circulator.schema import CirculatorArrival, CirculatorBusRuntimes, CirculatorRidership
from transitstat.circulator.reports import parse_args
from .factories import ArrivalFactory, RidershipFactory, RuntimesFactory


def test_factory(fakeSession):
    
    ArrivalFactory()
    ArrivalFactory(scheduled_arrival_time=time(12, 0))
    
    assert fakeSession.query(CirculatorArrival).count() == 2

def test_get_otp_no_force(ridesystems_reports):
    """Test for get_otp"""
    with patch('transitstat.circulator.reports.Reports.get_otp') as mocked_otp:
        df = {'date': [date(2022, 1, 1), date(2022, 1, 1), date(2022, 1, 1), date(2022, 1, 1)],
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

        return_df = pd.DataFrame(df)
        mocked_otp.return_value = return_df
        ridesystems_reports.get_otp(date(2022, 1, 1), date(2022, 1, 1), hours='12')

        with Session(bind=ridesystems_reports.engine, future=True) as session:
            ret = session.query(CirculatorArrival)
            assert ret.count() == 8

def test_get_otp_force(ridesystems_reports):
    """Test for get_otp with the force option on existing data"""
    with patch('transitstat.circulator.reports.Reports.get_otp') as mocked_otp:
        df = {'date': [date(2022, 1, 1), date(2022, 1, 1), date(2022, 1, 1)],
                'route': ['xx', 'xx', 'xx'],
                'stop': ['xx', 'xx', 'xx'],
                'blockid': ['xx', 'xx', 'xx'],
                'actualarrivaltime': [time(), time(), time()],
                'actualdeparturetime': [time(), time(), time()],
                'scheduledarrivaltime': [time(), time(1), time(2)],
                'scheduleddeparturetime': [time(), time(), time()],
                'vehicle': ['xx', 'xx', 'xx'],
                'ontimestatus': ['xx', 'xx', 'xx']
                }

        return_df = pd.DataFrame(df)
        mocked_otp.return_value = return_df
        ridesystems_reports.get_otp(date(2022, 1, 1), date(2022, 1, 1), force=True, hours='12')

        with Session(bind=ridesystems_reports.engine, future=True) as session:
            ret = session.query(CirculatorArrival)
            assert ret.count() == 7


def test_get_vehicle_assignments_no_force(ridesystems_reports):
    """Test get_vehicle_assignments"""
    with patch('transitstat.circulator.reports.Reports.get_runtimes') as mocked_vehicles:
        df = {'vehicle': ['1', '2', '3'],
                'route':['xx', 'xx', 'xx'],
                'start_time': [datetime(2022, 1, 1, 12, 0), datetime(2022, 1, 1, 12, 15), datetime(2022, 1, 1, 12, 30)],
                'end_time': [datetime(2022, 1, 1, 12, 0), datetime(2022, 1, 1, 12, 15), datetime(2022, 1, 1, 12, 30)]
                }

        return_df = pd.DataFrame(df)
        mocked_vehicles.return_value = return_df
        ridesystems_reports.get_vehicle_assignments(date(2022, 1, 1), date(2022, 1, 1))

        with Session(bind=ridesystems_reports.engine, future=True) as session:
            ret = session.query(CirculatorBusRuntimes)
            assert ret.count() == 7


def test_get_vehicle_assignments_force(ridesystems_reports):
    """Test get_vehicle_assignments with the force option on existing data"""
    with patch('transitstat.circulator.reports.Reports.get_runtimes') as mocked_vehicles:
        df = {'vehicle': ['Purple', 'Purple', 'Purple'],
                'route':['xx', 'xx', 'xx'],
                'start_time': [datetime(2021, 5, 1, 12, 0), datetime(2021, 5, 2, 12, 0), datetime(2021, 5, 3, 12, 0)],
                'end_time': [datetime(2022, 1, 1, 12, 0), datetime(2022, 1, 1, 12, 15), datetime(2022, 1, 1, 12, 30)]
                }

        return_df = pd.DataFrame(df)
        mocked_vehicles.return_value = return_df
        ridesystems_reports.get_vehicle_assignments(date(2022, 1, 1), date(2022, 1, 1), force=True)
        
        with Session(bind=ridesystems_reports.engine, future=True) as session:
            ret = session.query(CirculatorBusRuntimes)
            assert ret.count() == 4


def test_get_ridership_no_force(ridesystems_reports):
    """Test get_ridership"""
    with patch('transitstat.circulator.reports.Reports.get_ridership') as mocked_ridership:
        df = {'vehicle': ['Tesla Model S', 'bike', 'legs'],
                    'route': ['Purple', 'Purple', 'Purple'],
                    'stop': ["Webster's house", 'Brian St', 'Quilvio is cool'],
                    'latitude': [123.456789, 123.456789, 123.456789],
                    'longitude': [123.456789, 123.456789, 123.456789],
                    'datetime': [datetime(2022, 1, 1, 12, 0), datetime(2022, 1, 1, 12, 15), datetime(2022, 1, 1, 12, 30)],
                    'entries': [0, 3, 1],
                    'exits': [100, 0, 0],
                }

        return_df = pd.DataFrame(df)
        mocked_ridership.return_value = return_df
        ridesystems_reports.get_ridership(date(2022, 1, 1), date(2022, 1, 1))

        with Session(bind=ridesystems_reports.engine, future=True) as session:
            ret = session.query(CirculatorRidership)
            assert ret.count() == 7


def test_get_ridership_force(ridesystems_reports):
    """Test get_ridership"""
    with patch('transitstat.circulator.reports.Reports.get_ridership') as mocked_ridership:
        df = {'vehicle': ['xx1', 'xx2', 'xx3'],
                    'route': ['xx1', 'xx2', 'xx3'],
                    'stop': ['xx1', 'xx2', 'xx3'],
                    'latitude': [987.654321, 987.654321, 987.654321],
                    'longitude': [987.654321, 987.654321, 987.654321],
                    'datetime': [datetime(2021, 5, 1, 12, 0), datetime(2021, 5, 2, 12, 0), datetime(2021, 5, 3, 12, 0)],
                    'entries': [0, 3, 1],
                    'exits': [100, 0, 0],
                }

        return_df = pd.DataFrame(df)
        mocked_ridership.return_value = return_df
        ridesystems_reports.get_ridership(date(2022, 1, 1), date(2022, 1, 1), force=True)

        with Session(bind=ridesystems_reports.engine, future=True) as session:
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
