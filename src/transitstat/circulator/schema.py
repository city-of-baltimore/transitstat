"""Models used by Sql Alchemy"""
# pylint:disable=too-few-public-methods
from sqlalchemy import Column  # type: ignore
from sqlalchemy.ext.declarative import DeclarativeMeta  # type: ignore
from sqlalchemy.orm import declarative_base  # type: ignore
from sqlalchemy.types import Date, DateTime, Integer, String, Time  # type: ignore

Base: DeclarativeMeta = declarative_base()


class CirculatorRidership(Base):
    """Table holding the ridership by vehicle, route and date"""
    __tablename__ = "ccc_aggregate_ridership_manual"

    ridership_date = Column(Date, primary_key=True)
    route = Column(String(length=10), primary_key=True)
    block_id = Column(Integer, primary_key=True)
    riders = Column(Integer)


class CirculatorArrival(Base):
    """Table holding circulator arrival times and on time status"""
    __tablename__ = "ccc_arrival_times"

    date = Column(Date, primary_key=True)
    route = Column(String(length=50), primary_key=True)
    stop = Column(String)
    block_id = Column(String(length=100), primary_key=True)
    scheduled_arrival_time = Column(Time, primary_key=True)
    actual_arrival_time = Column(Time)
    scheduled_departure_time = Column(Time)
    actual_departure_time = Column(Time)
    on_time_status = Column(String(length=10))
    vehicle = Column(String(length=20))


class CirculatorBusRuntimes(Base):
    """Table holding the realtime runtimes of the circulator"""
    __tablename__ = "ccc_bus_runtimes"

    busid = Column(String(length=10), primary_key=True)
    route = Column(String)
    starttime = Column(DateTime, primary_key=True)
    endtime = Column(DateTime)
