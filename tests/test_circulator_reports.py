"""Test suites for circulator.circulator_reports"""
from datetime import date

from sqlalchemy.orm import Session  # type: ignore

from transitstat.circulator.schema import CirculatorArrival, CirculatorBusRuntimes, CirculatorRidership
from transitstat.circulator.reports import parse_args


def test_get_otp(ridesystems_reports):
    """Test for get_otp"""
    ridesystems_reports.get_otp(date(2021, 6, 1), date(2021, 6, 1), hours='12')
    with Session(bind=ridesystems_reports.engine, future=True) as session:
        ret = session.query(CirculatorArrival)
        assert ret.count() > 100


def test_get_otp_force(ridesystems_reports):
    """Test for get_otp with the force option on existing data"""
    ridesystems_reports.get_otp(date(2021, 5, 1), date(2021, 5, 1), force=True, hours='12')
    with Session(bind=ridesystems_reports.engine, future=True) as session:
        ret = session.query(CirculatorArrival)
        assert ret.count() > 100


def test_get_otp_no_force(ridesystems_reports):
    """Test for get_otp disabling the force option on existing data"""
    ridesystems_reports.get_otp(date(2021, 5, 1), date(2021, 5, 1), force=False)
    with Session(bind=ridesystems_reports.engine, future=True) as session:
        ret = session.query(CirculatorArrival)
        assert ret.count() == 4


def test_get_vehicle_assignments(ridesystems_reports):
    """Test get_vehicle_assignments"""
    ridesystems_reports.get_vehicle_assignments(date(2021, 6, 1), date(2021, 6, 1))
    with Session(bind=ridesystems_reports.engine, future=True) as session:
        ret = session.query(CirculatorBusRuntimes)
        assert ret.count() == 23


def test_get_vehicle_assignments_force(ridesystems_reports):
    """Test get_vehicle_assignments with the force option on existing data"""
    ridesystems_reports.get_vehicle_assignments(date(2021, 5, 1), date(2021, 5, 1), force=True)
    with Session(bind=ridesystems_reports.engine, future=True) as session:
        ret = session.query(CirculatorBusRuntimes)
        assert ret.count() > 100


def test_get_vehicle_assignments_no_force(ridesystems_reports):
    """get_vehicle_assignments disabling the force option on existing data"""
    ridesystems_reports.get_vehicle_assignments(date(2021, 5, 1), date(2021, 5, 1), force=False)
    with Session(bind=ridesystems_reports.engine, future=True) as session:
        ret = session.query(CirculatorBusRuntimes)
        assert ret.count() == 4


def test_get_ridership(ridesystems_reports):
    """Test get_ridership"""
    ridesystems_reports.get_ridership(date(2021, 6, 1), date(2021, 6, 1))
    with Session(bind=ridesystems_reports.engine, future=True) as session:
        ret = session.query(CirculatorRidership)
        assert ret.count() > 1000


def test_get_ridership_force(ridesystems_reports):
    """Test get_ridership"""
    ridesystems_reports.get_ridership(date(2021, 5, 1), date(2021, 5, 1), force=True)
    with Session(bind=ridesystems_reports.engine, future=True) as session:
        ret = session.query(CirculatorRidership)
        assert ret.count() > 1000


def test_get_ridership_no_force(ridesystems_reports):
    """Test get_ridership"""
    ridesystems_reports.get_ridership(date(2021, 5, 1), date(2021, 5, 1), force=False)
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
