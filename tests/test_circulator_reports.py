"""Test suites for circulator.circulator_reports"""
from datetime import date, timedelta
from unittest.mock import patch

import pytest
import pandas as pd  # type: ignore
from sqlalchemy.orm import Session  # type: ignore

from transitstat.circulator.schema import CirculatorArrival, CirculatorBusRuntimes, CirculatorRidership
from transitstat.circulator.reports import parse_args, RidesystemReports


def body_testing(mocked_rs_cls, conn_str: str, func, factory, force: bool = False):
    """Repeated code chunk to use for the different tests"""
    test_df = pd.DataFrame(data=factory.create_batch(100))

    inst = RidesystemReports(conn_str, 'username', 'superdupersecretpassword')
    date_start = date.today() - timedelta(days=2)
    date_end = date.today()

    if func == 'otp':
        inst.rs_cls.get_otp.return_value = test_df  # type: ignore
        model = CirculatorArrival
        date_column = model.date

        inst.get_otp(date_start, date_end, force=force, hours='12')
    elif func == 'runtime':
        inst.rs_cls.get_runtimes.return_value = test_df  # type: ignore
        model = CirculatorBusRuntimes
        date_column = model.starttime

        inst.get_vehicle_assignments(date_start, date_end, force=force)
    elif func == 'ridership':
        inst.rs_cls.get_ridership.return_value = test_df  # type: ignore
        model = CirculatorRidership
        date_column = model.datetime

        inst.get_ridership(date_start, date_end, force=force)
    else:
        assert False

    mocked_rs_cls.assert_called_once_with('username', 'superdupersecretpassword')

    with Session(bind=inst.engine, future=True) as session:
        ret = session.query(model)
        assert ret.count() == 100

    if force:
        assert len(inst.get_dates_to_process(date_start, date_end, date_column, force)) == 3
    else:
        assert len(inst.get_dates_to_process(date_start, date_end, date_column, force)) == 0


@pytest.mark.filterwarnings("ignore::sqlalchemy.exc.SAWarning")
@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_otp_no_force(mocked_rs_cls, conn_str: str, arrival_dataset):
    """Test for get_otp without force"""

    body_testing(mocked_rs_cls, conn_str, 'otp', arrival_dataset, force=False)


@pytest.mark.filterwarnings("ignore::sqlalchemy.exc.SAWarning")
@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_otp_force(mocked_rs_cls, conn_str: str, arrival_dataset):
    """Test for get_otp"""
    body_testing(mocked_rs_cls, conn_str, 'otp', arrival_dataset, force=True)


@pytest.mark.filterwarnings("ignore::sqlalchemy.exc.SAWarning")
@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_vehicle_assignments_no_force(mocked_rs_cls, conn_str: str, runtime_dataset):
    """Test get_vehicle_assignments"""
    body_testing(mocked_rs_cls, conn_str, 'runtime', runtime_dataset, force=False)


@pytest.mark.filterwarnings("ignore::sqlalchemy.exc.SAWarning")
@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_vehicle_assignments_force(mocked_rs_cls, conn_str, runtime_dataset):
    """Test get_vehicle_assignments with the force option on existing data"""
    body_testing(mocked_rs_cls, conn_str, 'runtime', runtime_dataset, force=True)


@pytest.mark.filterwarnings("ignore::sqlalchemy.exc.SAWarning")
@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_ridership_no_force(mocked_rs_cls, conn_str, ridership_dataset):
    """Test get_ridership"""
    body_testing(mocked_rs_cls, conn_str, 'ridership', ridership_dataset, force=False)


@pytest.mark.filterwarnings("ignore::sqlalchemy.exc.SAWarning")
@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_ridership_force(mocked_rs_cls, conn_str, ridership_dataset):
    """Test get_ridership"""
    body_testing(mocked_rs_cls, conn_str, 'ridership', ridership_dataset, force=True)


@patch('transitstat.circulator.reports.RideSystemsInterface')
def test_get_dates_to_process(mocked_rs_cls, conn_str, arrival_dataset, runtime_dataset):
    """Test get_dates_to_process"""
    arrival_df = pd.DataFrame(data=arrival_dataset.create_batch(100))
    runtime_df = pd.DataFrame(data=runtime_dataset.create_batch(100))

    inst = RidesystemReports(conn_str, 'username', 'superdupersecretpassword')
    inst.rs_cls.get_otp.return_value = arrival_df
    inst.rs_cls.get_runtimes.return_value = runtime_df
    assert mocked_rs_cls.assert_called_once_with('username', 'superdupersecretpassword') is None

    date_start = date.today() - timedelta(days=2)
    date_end = date.today()

    inst.get_otp(date_start, date_end, hours='12')
    inst.get_vehicle_assignments(date_start, date_end)
    # Test with datetime
    dates = inst.get_dates_to_process(date_start, date_end,
                                      CirculatorBusRuntimes.starttime, force=False)
    assert len(dates) == 0

    dates = inst.get_dates_to_process(date_start, date_end,
                                      CirculatorBusRuntimes.starttime, force=True)
    assert len(dates) == 3

    # Test with date
    dates = inst.get_dates_to_process(date_start, date_end,
                                      CirculatorArrival.date, force=False)
    assert len(dates) == 0

    dates = inst.get_dates_to_process(date_start, date_end,
                                      CirculatorArrival.date, force=True)
    assert len(dates) == 3


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
