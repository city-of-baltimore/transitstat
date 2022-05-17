"""Pytest fixtures"""
from datetime import datetime, time
from collections import namedtuple
from random import randint

import pytest
from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.orm import scoped_session, sessionmaker  # type: ignore
from factory.alchemy import SQLAlchemyModelFactory  # type: ignore
from factory import Sequence, Faker, LazyAttribute, Factory  # type: ignore

from transitstat.circulator.import_ridership import DataImporter
from transitstat.circulator.schema import Base, CirculatorArrival, CirculatorBusRuntimes, CirculatorRidership
from transitstat.connector.data_import import ConnectorImport


@pytest.fixture
def conn_str(tmp_path_factory):
    """Connection string for engine"""
    url = f"sqlite:///{str(tmp_path_factory.mktemp('data') / 'transitstat.db')}"
    engine = create_engine(url, echo=True, future=True)
    with engine.begin() as connection:
        Base.metadata.create_all(connection)

    return url


@pytest.fixture
def session(conn_str):  # pylint: disable=redefined-outer-name
    """Session for factories"""
    engine = create_engine(conn_str, echo=True, future=True)
    common_session = scoped_session(sessionmaker(bind=engine))
    yield common_session
    common_session.rollback()  # pylint: disable=no-member
    common_session.close()  # pylint: disable=no-member


@pytest.fixture
def arrival_factory(session):  # pylint: disable=redefined-outer-name
    """Generate fake CCC arrival db"""
    class ArrivalFactory(SQLAlchemyModelFactory):
        """Factory to generate fake CCC arrival data"""
        class Meta:  # pylint: disable=too-few-public-methods
            """Model and session for factory"""
            model = CirculatorArrival
            sqlalchemy_session = session

        date = Faker('date_between_dates')
        route = Faker('safe_color_name')
        stop = Faker('street_name')
        block_id = LazyAttribute(lambda obj: f'{str.upper(obj.route[0])}_{randint(1, 6)}')
        scheduled_arrival_time = Sequence(lambda n: time(n % 24, n % 60))

    return ArrivalFactory


@pytest.fixture
def runtime_factory(session):  # pylint: disable=redefined-outer-name
    """Generate fake CCC runtime db"""
    class RuntimeFactory(SQLAlchemyModelFactory):
        """Factory to generate fake CCC runtime data"""
        class Meta:  # pylint: disable=too-few-public-methods
            """Model and session for factory"""
            model = CirculatorBusRuntimes
            sqlalchemy_session = session

        busid = 'CC1212'
        route = Faker('safe_color_name')
        starttime = Sequence(lambda n: datetime(2022, 1, 2, n % 24, n % 60, n % 60))
        endtime = Sequence(lambda n: datetime(2022, 1, 2, n % 24, n % 60, n % 60))

    return RuntimeFactory


@pytest.fixture
def ridership_factory(session):  # pylint: disable=redefined-outer-name
    """Generate fake CCC ridership db"""
    class RidershipFactory(SQLAlchemyModelFactory):
        """Factory to generate fake CCC ridership data"""
        class Meta:  # pylint: disable=too-few-public-methods
            """Model and session for factory"""
            model = CirculatorRidership
            sqlalchemy_session = session

        vehicle = 'CC1212'
        route = Faker('safe_color_name')
        stop = Faker('street_name')
        latitude = Faker('latitude')
        longitude = Faker('longitude')
        datetime = Sequence(lambda n: datetime(2022, 1, 2, n % 24, n % 60, n % 60))
        boardings = Faker('random_number', digits=2)
        alightings = Faker('random_number', digits=2)

    return RidershipFactory


@pytest.fixture
def arrival_dataset():
    """Generate fake CCC arrival data"""
    Dataset = namedtuple("Dataset",
                         ['date', 'route', 'stop', 'blockid', 'actualarrivaltime',
                          'actualdeparturetime', 'scheduledarrivaltime', 'scheduleddeparturetime',
                          'vehicle', 'ontimestatus'])

    class ArrivalDatasetFactory(Factory):
        """Factory for generating fake CCC arrival data"""
        class Meta:  # pylint: disable=too-few-public-methods
            """Model for factory"""
            model = Dataset

        date = Faker('date_between_dates')
        route = Faker('safe_color_name')
        stop = Faker('street_name')
        blockid = LazyAttribute(lambda obj: f'{str.upper(obj.route[0])}_{randint(1, 6)}')
        actualarrivaltime = Sequence(lambda n: time(n % 24, n % 60))
        scheduledarrivaltime = Sequence(lambda n: time(n % 24, n % 60))
        scheduleddeparturetime = Sequence(lambda n: time(n % 24, n % 60))
        actualdeparturetime = Sequence(lambda n: time(n % 24, n % 60))
        vehicle = 'CC1211'
        ontimestatus = 'On Time'

    return ArrivalDatasetFactory


@pytest.fixture
def runtime_dataset():
    """Generate fake CCC runtime data"""
    Dataset = namedtuple("Dataset", ['route', 'vehicle', 'start_time', 'end_time'])

    class RuntimeDatasetFactory(Factory):
        """Factory for generating fake CCC runtime data"""
        class Meta:  # pylint: disable=too-few-public-methods
            """Model for factory"""
            model = Dataset

        vehicle = Faker('license_plate')
        route = Faker('safe_color_name')
        start_time = Sequence(lambda n: datetime(2022, 1, 1, n % 24, n % 60, n % 60))
        end_time = Sequence(lambda n: datetime(2022, 1, 1, n % 24, n % 60, n % 60))

    return RuntimeDatasetFactory


@pytest.fixture
def ridership_dataset():
    """Generate fake CCC ridership data"""
    Dataset = namedtuple("Dataset",
                         ['vehicle', 'route', 'stop', 'datetime',
                          'latitude', 'longitude', 'entries', 'exits'])

    class RidershipDatasetFactory(Factory):
        """Factory for generating fake CCC ridership data"""
        class Meta:  # pylint: disable=too-few-public-methods
            """Model for factory"""
            model = Dataset

        vehicle = 'CC1212'
        route = Faker('safe_color_name')
        stop = Faker('street_name')
        latitude = Faker('latitude')
        longitude = Faker('longitude')
        datetime = Sequence(lambda n: datetime(2022, 1, 1, n % 24, n % 60, n % 60))
        entries = Faker('random_number', digits=2)
        exits = Faker('random_number', digits=2)

    return RidershipDatasetFactory


@pytest.fixture(name='dataimporter')
def fixture_dataimporter(conn_str):  # pylint: disable=redefined-outer-name
    """transitstat.circulator.import_ridership.DataImporter fixture"""
    return DataImporter(conn_str)


@pytest.fixture(name='connector_import')
def fixture_connector_import(conn_str):  # pylint: disable=redefined-outer-name
    """transitstat.connector.data_import.ConnectorImport"""
    return ConnectorImport(conn_str)
