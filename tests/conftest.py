"""Pytest fixtures"""
from datetime import date, datetime, time

import pytest
from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.orm import Session  # type: ignore

from transitstat.circulator.import_ridership import DataImporter
from transitstat.circulator.reports import RidesystemReports
from transitstat.circulator.schema import Base, CirculatorArrival, CirculatorBusRuntimes, CirculatorRidership


def pytest_addoption(parser):
    """Pytest custom arguments"""
    parser.addoption('--ridesystems-user', action='store')
    parser.addoption('--ridesystems-pass', action='store')


@pytest.fixture(scope='session', name='ridesystems_user')
def fixture_ridesystems_username(request):
    """The username to login to Ridesystems"""
    return request.config.getoption('--ridesystems-user')


@pytest.fixture(scope='session', name='ridesystems_password')
def fixture_ridesystem_password(request):
    """The password to login to Ridesystems"""
    return request.config.getoption('--ridesystems-pass')


@pytest.fixture(name='conn_str')
def fixture_conn_str(tmp_path_factory):
    """Fixture for the WorksheetMaker class"""
    conn_str = 'sqlite:///{}'.format(str(tmp_path_factory.mktemp("data") / 'transitstat.db'))
    engine = create_engine(conn_str, echo=True, future=True)
    with engine.begin() as connection:
        Base.metadata.create_all(connection)

    with Session(bind=engine) as session:
        session.add_all([
            # Circulator Arrival
            CirculatorArrival(
                date=date(2021, 5, 1),
                route='xx',
                block_id='xx',
                scheduled_arrival_time=time()),
            CirculatorArrival(
                date=date(2021, 5, 2),
                route='xx',
                block_id='xx',
                scheduled_arrival_time=time()),
            CirculatorArrival(
                date=date(2021, 5, 3),
                route='xx',
                block_id='xx',
                scheduled_arrival_time=time()),
            CirculatorArrival(
                date=date(2021, 5, 4),
                route='xx',
                block_id='xx',
                scheduled_arrival_time=time()),

            # Circulator Bus Runtimes
            CirculatorBusRuntimes(
                busid='Purple',
                route='xx',
                starttime=datetime(2021, 5, 1, 12, 0),
                endtime=datetime(2021, 5, 1, 12, 0)),
            CirculatorBusRuntimes(
                busid='Purple',
                route='xx',
                starttime=datetime(2021, 5, 2, 12, 0),
                endtime=datetime(2021, 5, 2, 12, 0)),
            CirculatorBusRuntimes(
                busid='Purple',
                route='xx',
                starttime=datetime(2021, 5, 3, 12, 0),
                endtime=datetime(2021, 5, 3, 12, 0)),
            CirculatorBusRuntimes(
                busid='Purple',
                route='xx',
                starttime=datetime(2021, 5, 4, 12, 0),
                endtime=datetime(2021, 5, 4, 12, 0)),

            # Circulator Ridership
            CirculatorRidership(
                vehicle='xx1',
                route='xx1',
                stop='xx1',
                latitude=123.456789,
                longitude=123.456789,
                datetime=datetime(2021, 5, 1, 12, 0),
                boardings=30,
                alightings=30
            ),
            CirculatorRidership(
                vehicle='xx2',
                route='xx2',
                stop='xx2',
                latitude=123.456789,
                longitude=123.456789,
                datetime=datetime(2021, 5, 2, 12, 0),
                boardings=30,
                alightings=30
            ),
            CirculatorRidership(
                vehicle='xx3',
                route='xx3',
                stop='xx3',
                latitude=123.456789,
                longitude=123.456789,
                datetime=datetime(2021, 5, 3, 12, 0),
                boardings=30,
                alightings=30
            ),
            CirculatorRidership(
                vehicle='xx4',
                route='xx4',
                stop='xx4',
                latitude=123.456789,
                longitude=123.456789,
                datetime=datetime(2021, 5, 4, 12, 0),
                boardings=30,
                alightings=30
            ),
        ])

        session.commit()
    return conn_str


@pytest.fixture(name='dataimporter')
def fixture_dataimporter(conn_str):
    """transitstat.circulator.import_ridership.DataImporter fixture"""
    return DataImporter(conn_str)


@pytest.fixture(name='ridesystems_reports')
def fixture_ridesystems_reports(conn_str, ridesystems_user, ridesystems_password):
    """transitstat.circulator.reports.RidesystemsReports fixture"""
    return RidesystemReports(conn_str, ridesystems_user, ridesystems_password)
