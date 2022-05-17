"""Test suites for circulator.circulator_reports"""
from datetime import date
from unittest.mock import patch

import pytest
import pandas as pd  # type: ignore
from sqlalchemy.orm import Session  # type: ignore

from transitstat.circulator.schema import CirculatorArrival, CirculatorBusRuntimes, CirculatorRidership
from transitstat.circulator.reports import parse_args, RidesystemReports


@pytest.mark.filterwarnings("ignore::sqlalchemy.exc.SAWarning")
@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_otp_no_force(mocked_rs_cls, conn_str, arrival_factory, arrival_dataset):
    """Test for get_otp without force"""

    # arrival_factory.create_batch(100)
    test_df = pd.DataFrame(data=arrival_dataset.create_batch(10))

    inst = RidesystemReports(conn_str, 'username', 'superdupersecretpassword')
    inst.rs_cls.get_otp.return_value = test_df

    with Session(bind=inst.engine, future=True) as session:
        batch = arrival_factory.create_batch(100)
        session.bulk_save_objects(batch)
        session.commit()

    inst.get_otp(date(2022, 1, 1), date(2022, 1, 1), hours='12')
    assert mocked_rs_cls.assert_called_once_with('username', 'superdupersecretpassword') is None
    assert inst.rs_cls.get_otp.assert_called_once_with(date(2022, 1, 1), date(2022, 1, 1), hours='12') is None  # pylint: disable=no-member

    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorArrival)
        assert ret.count() == 110

    test_df['stop'] = '25th St'
    inst.get_otp(date(2022, 1, 1), date(2022, 1, 1))
    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorArrival)
        assert ret.count() == 110

    assert ['25th St' for _ in range(10)] != [r.stop for r in session.query(CirculatorRidership).all()[100:110]]


@pytest.mark.filterwarnings("ignore::sqlalchemy.exc.SAWarning")
@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_otp_force(mocked_rs_cls, conn_str, arrival_factory, arrival_dataset):
    """Test for get_otp"""
    test_df = pd.DataFrame(data=arrival_dataset.create_batch(10))

    inst = RidesystemReports(conn_str, 'username', 'superdupersecretpassword')
    inst.rs_cls.get_otp.return_value = test_df

    with Session(bind=inst.engine, future=True) as session:
        batch = arrival_factory.create_batch(100)
        session.bulk_save_objects(batch)
        session.commit()

    inst.get_otp(date(2022, 1, 1), date(2022, 1, 1), force=True, hours='12')
    assert mocked_rs_cls.assert_called_once_with('username', 'superdupersecretpassword') is None
    assert inst.rs_cls.get_otp.assert_called_once_with(date(2022, 1, 1), date(2022, 1, 1), hours='12') is None  # pylint: disable=no-member

    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorArrival)
        assert ret.count() == 110

    test_df['stop'] = '25th St'
    inst.get_otp(date(2022, 1, 1), date(2022, 1, 1), force=True, hours='12')
    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorArrival)
        assert ret.count() == 110

    assert ['25th St' for _ in range(10)] != [r.stop for r in session.query(CirculatorRidership).all()[100:110]]


@pytest.mark.filterwarnings("ignore::sqlalchemy.exc.SAWarning")
@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_vehicle_assignments_no_force(mocked_rs_cls, conn_str, runtime_factory, runtime_dataset):
    """Test get_vehicle_assignments"""
    test_df = pd.DataFrame(data=runtime_dataset.create_batch(10))

    inst = RidesystemReports(conn_str, 'username', 'superdupersecretpassword')
    inst.rs_cls.get_runtimes.return_value = test_df

    with Session(bind=inst.engine, future=True) as session:
        batch = runtime_factory.create_batch(100)
        session.bulk_save_objects(batch)
        session.commit()

    inst.get_vehicle_assignments(date(2022, 1, 1), date(2022, 1, 1))
    assert mocked_rs_cls.assert_called_once_with('username', 'superdupersecretpassword') is None
    assert inst.rs_cls.get_runtimes.assert_called_once_with(date(2022, 1, 1), date(2022, 1, 1)) is None  # pylint: disable=no-member

    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorBusRuntimes)
        assert ret.count() == 110

    test_df['route'] = 'blue'
    inst.get_vehicle_assignments(date(2022, 1, 1), date(2022, 1, 1))
    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorBusRuntimes)
        assert ret.count() == 110

    assert ['blue' for _ in range(10)] != [r.route for r in session.query(CirculatorRidership).all()[100:110]]


@pytest.mark.filterwarnings("ignore::sqlalchemy.exc.SAWarning")
@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_vehicle_assignments_force(mocked_rs_cls, conn_str, runtime_factory, runtime_dataset):
    """Test get_vehicle_assignments with the force option on existing data"""
    test_df = pd.DataFrame(data=runtime_dataset.create_batch(10))

    inst = RidesystemReports(conn_str, 'username', 'superdupersecretpassword')
    inst.rs_cls.get_runtimes.return_value = test_df

    with Session(bind=inst.engine, future=True) as session:
        batch = runtime_factory.create_batch(100)
        session.bulk_save_objects(batch)
        session.commit()

    inst.get_vehicle_assignments(date(2022, 1, 1), date(2022, 1, 1), force=True)
    assert mocked_rs_cls.assert_called_once_with('username', 'superdupersecretpassword') is None
    assert inst.rs_cls.get_runtimes.assert_called_once_with(date(2022, 1, 1), date(2022, 1, 1)) is None  # pylint: disable=no-member

    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorBusRuntimes)
        assert ret.count() == 110

    test_df['route'] = 'blue'
    inst.get_vehicle_assignments(date(2022, 1, 1), date(2022, 1, 1), force=True)
    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorBusRuntimes)
        assert ret.count() == 110

    assert ['blue' for _ in range(10)] == [r.route for r in session.query(CirculatorBusRuntimes).all()[100:110]]


@pytest.mark.filterwarnings("ignore::sqlalchemy.exc.SAWarning")
@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_ridership_no_force(mocked_rs_cls, conn_str, ridership_factory, ridership_dataset):
    """Test get_ridership"""
    test_df = pd.DataFrame(data=ridership_dataset.create_batch(10))

    inst = RidesystemReports(conn_str, 'username', 'superdupersecretpassword')
    inst.rs_cls.get_ridership.return_value = test_df

    with Session(bind=inst.engine, future=True) as session:
        batch = ridership_factory.create_batch(100)
        session.bulk_save_objects(batch)
        session.commit()

    inst.get_ridership(date(2022, 1, 1), date(2022, 1, 1))
    assert mocked_rs_cls.assert_called_once_with('username', 'superdupersecretpassword') is None
    assert inst.rs_cls.get_ridership.assert_called_once_with(date(2022, 1, 1), date(2022, 1, 1)) is None  # pylint: disable=no-member

    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorRidership)
        assert ret.count() == 110

    test_df['entries'] = 1
    inst.get_ridership(date(2022, 1, 1), date(2022, 1, 1))
    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorRidership)
        assert ret.count() == 110

    assert [1 for _ in range(10)] != [r.boardings for r in session.query(CirculatorRidership).all()[100:110]]


@pytest.mark.filterwarnings("ignore::sqlalchemy.exc.SAWarning")
@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_ridership_force(mocked_rs_cls, conn_str, ridership_factory, ridership_dataset):
    """Test get_ridership"""
    test_df = pd.DataFrame(data=ridership_dataset.create_batch(10))

    inst = RidesystemReports(conn_str, 'username', 'superdupersecretpassword')
    inst.rs_cls.get_ridership.return_value = test_df

    with Session(bind=inst.engine, future=True) as session:
        batch = ridership_factory.create_batch(100)
        session.bulk_save_objects(batch)
        session.commit()

    inst.get_ridership(date(2022, 1, 1), date(2022, 1, 1), force=True)
    assert mocked_rs_cls.assert_called_once_with('username', 'superdupersecretpassword') is None
    assert inst.rs_cls.get_ridership.assert_called_once_with(date(2022, 1, 1), date(2022, 1, 1)) is None  # pylint: disable=no-member

    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorRidership)
        assert ret.count() == 110

    test_df['entries'] = 1
    inst.get_ridership(date(2022, 1, 1), date(2022, 1, 1), force=True)
    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(CirculatorRidership)
        assert ret.count() == 110

    assert [1 for _ in range(10)] == [r.boardings for r in session.query(CirculatorRidership).all()[100:110]]


@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_dates_to_process(mocked_rs_cls, conn_str, arrival_factory, runtime_factory):
    """Test get_dates_to_process"""

    inst = RidesystemReports(conn_str, 'username', 'superdupersecretpassword')
    assert mocked_rs_cls.assert_called_once_with('username', 'superdupersecretpassword') is None

    with Session(bind=inst.engine, future=True) as session:
        batch = arrival_factory.create_batch(100)
        session.bulk_save_objects(batch)
        session.commit()

    with Session(bind=inst.engine, future=True) as session:
        batch = runtime_factory.create_batch(100)
        session.bulk_save_objects(batch)
        session.commit()

    # Test with datetime
    dates = inst.get_dates_to_process(date(2022, 1, 2), date(2022, 1, 2),
                                      CirculatorBusRuntimes.starttime, force=False)
    assert len(dates) == 0

    dates = inst.get_dates_to_process(date(2022, 1, 2), date(2022, 1, 2),
                                      CirculatorBusRuntimes.starttime, force=True)
    assert len(dates) == 1

    # Test with date
    dates = inst.get_dates_to_process(date.today(), date.today(),
                                      CirculatorArrival.date, force=False)
    assert len(dates) == 0

    dates = inst.get_dates_to_process(date.today(), date.today(),
                                      CirculatorArrival.date, force=True)
    assert len(dates) == 1


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
