"""
SQLAlchemy models - defines table for storing scraped data.

"""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String


DeclarativeBase = declarative_base()


def create_db_table(engine):
	DeclarativeBase.metadata.create_all(engine)


class SQLItem(DeclarativeBase):
	"""SQLAlchemy item model"""
	__tablename__ = "items"

	id = Column(Integer, primary_key=True)
#	title = Column('title', String)
#	description = Column('description', String, nullable=True)
#	link = Column('link', String, nullable=True)
#	location = Column('location', String, nullable=True)
#	category = Column('category', String, nullable=True)
#	original_price = Column('original_price', String, nullable=True)
#	price = Column('price', String, nullable=True)
