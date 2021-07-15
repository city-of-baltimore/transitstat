"""Pytest fixtures"""
import pytest
from sqlalchemy import create_engine  # type: ignore

from transitstat.circulator.schema import Base  # type: ignore
from transitstat.circulator.import_ridership import DataImporter  # type: ignore


@pytest.fixture(name='conn_str')
def fixture_conn_str(tmp_path_factory):
    """Fixture for the WorksheetMaker class"""
    conn_str = 'sqlite:///{}'.format(str(tmp_path_factory.mktemp("data") / 'transitstat.db'))
    engine = create_engine(conn_str, echo=True, future=True)
    with engine.begin() as connection:
        Base.metadata.create_all(connection)

    return conn_str


@pytest.fixture(name='dataimporter')
def fixture_dataimporter(conn_str):
    """transitstat.circulator.import_ridership.DataImporter fixture"""
    return DataImporter(conn_str)
