"""Test suites for circulator.circulator_reports"""
from datetime import date

from sqlalchemy.orm import Session  # type: ignore

from transitstat.circulator.schema import CirculatorArrival, CirculatorBusRuntimes


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
