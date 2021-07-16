"""Pytest fixtures"""
from datetime import date, time

import pytest
from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.orm import Session  # type: ignore

from transitstat.circulator.schema import Base, CirculatorArrival
from transitstat.circulator.import_ridership import DataImporter


@pytest.fixture(name='conn_str')
def fixture_conn_str(tmp_path_factory):
    """Fixture for the WorksheetMaker class"""
    conn_str = 'sqlite:///{}'.format(str(tmp_path_factory.mktemp("data") / 'transitstat.db'))
    engine = create_engine(conn_str, echo=True, future=True)
    with engine.begin() as connection:
        Base.metadata.create_all(connection)

    with Session(bind=engine) as session:
        session.add_all([
            CirculatorArrival(date=date(2021, 5, 1),
                              route='xx',
                              block_id='xx',
                              scheduled_arrival_time=time()),
            CirculatorArrival(date=date(2021, 5, 2),
                              route='xx',
                              block_id='xx',
                              scheduled_arrival_time=time()),
            CirculatorArrival(date=date(2021, 5, 3),
                              route='xx',
                              block_id='xx',
                              scheduled_arrival_time=time()),
            CirculatorArrival(date=date(2021, 5, 4),
                              route='xx',
                              block_id='xx',
                              scheduled_arrival_time=time()),
        ])
        session.commit()
    return conn_str


@pytest.fixture(name='dataimporter')
def fixture_dataimporter(conn_str):
    """transitstat.circulator.import_ridership.DataImporter fixture"""
    return DataImporter(conn_str)
