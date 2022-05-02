from datetime import date, time
from random import randint

from factory.alchemy import SQLAlchemyModelFactory

from src.transitstat.circulator import schema
from . import common

class ArrivalFactory(SQLAlchemyModelFactory):
    class Meta:
        model = schema.CirculatorArrival
        sqlalchemy_session = common.Session
    
    date = date(2022, 1, 1)
    route = 'Purple'
    stop = 'London St.'
    block_id = '1'
    scheduled_arrival_time = time()


class RuntimesFactory(SQLAlchemyModelFactory):
    class Meta:
        model = schema.CirculatorBusRuntimes
        sqlalchemy_session = common.Session

    # TODO default Runtime values


class RidershipFactory(SQLAlchemyModelFactory):
    class Meta:
        model = schema.CirculatorRidership
        sqlalchemy_session = common.Session

    # TODO default Ridership values