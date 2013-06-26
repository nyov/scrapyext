from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL

import settings

DeclarativeBase = declarative_base()

def db_connect():
	"""Performs database connection using database settings from settings.py.

	Returns sqlalchemy engine instance.
	"""
	return create_engine(URL(**settings.DATABASE))

def create_db_table(engine):
	DeclarativeBase.metadata.create_all(engine)


### Database Model ###
class Product(DeclarativeBase):
	__tablename__ = "product"

	id = Column(Integer, primary_key=True)
#	title = Column('title', String)
#	description = Column('description', String, nullable=True)
#	link = Column('link', String, nullable=True)
#	location = Column('location', String, nullable=True)
#	category = Column('category', String, nullable=True)
#	original_price = Column('original_price', String, nullable=True)
#	price = Column('price', String, nullable=True)

class Manufacturer(DeclarativeBase):
	__tablename__ = "manufacturer"

	id = Column(Integer, primary_key=True)
