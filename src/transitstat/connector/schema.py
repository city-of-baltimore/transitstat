"""Models used by Sql Alchemy"""
# pylint:disable=too-few-public-methods
from sqlalchemy import Column  # type: ignore
from sqlalchemy.ext.declarative import DeclarativeMeta  # type: ignore
from sqlalchemy.orm import declarative_base  # type: ignore
from sqlalchemy.types import Date, Integer  # type: ignore

Base: DeclarativeMeta = declarative_base()


class HcRidership(Base):
    """Ridership numbers for the Harbor Connector"""
    __tablename__ = 'hc_ridership'

    route_id = Column(Integer, primary_key=True)
    date = Column(Date, primary_key=True)
    riders = Column(Integer)
