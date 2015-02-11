from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL

import settings


Base = declarative_base()


def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(URL(**settings.DATABASE))


def create_resource_table(engine):
    """"""
    Base.metadata.create_all(engine)


class Resource(Base):
    """Sqlalchemy resource model"""
    __tablename__ = "resource"

    id = Column(Integer, primary_key=True)
    name = Column('name', String)
    original_price = Column('original_price', String, nullable=True)
    status = Column('status', String, nullable=True)
