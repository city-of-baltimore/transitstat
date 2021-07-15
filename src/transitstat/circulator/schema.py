"""Models used by Sql Alchemy"""
# pylint:disable=too-few-public-methods
from sqlalchemy import Column  # type: ignore
from sqlalchemy.ext.declarative import DeclarativeMeta  # type: ignore
from sqlalchemy.ext.declarative import declarative_base  # type: ignore
from sqlalchemy.types import Date, Integer, String  # type: ignore

Base: DeclarativeMeta = declarative_base()


class CirculatorRidership(Base):
    """Table holding the ridership by vehicle, route and date"""
    __tablename__ = "ccc_aggregate_ridership_manual"

    RidershipDate = Column(Date, primary_key=True)
    Route = Column(String(length=10), primary_key=True)
    BlockID = Column(Integer, primary_key=True)
    Riders = Column(Integer)
