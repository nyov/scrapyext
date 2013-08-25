"""
SQLAlchemy models - defines table for storing scraped data.

"""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String


DeclarativeBase = declarative_base()

def create_db_tables(engine):
	"""Create database tables"""
	DeclarativeBase.metadata.create_all(engine)

# SQLAlchemy models
class SQLItem(DeclarativeBase):
	"""SQLAlchemy item model"""
	__tablename__ = "items"

	# sql (relational) requirements
	id = Column(Integer, primary_key=True)

#	title = Column('title', String)
#	description = Column('description', String, nullable=True)
#	link = Column('link', String, nullable=True)
#	location = Column('location', String, nullable=True)
#	category = Column('category', String, nullable=True)
#	original_price = Column('original_price', String, nullable=True)
#	price = Column('price', String, nullable=True)

class SQLArticle(DeclarativeBase):
	"""SQLAlchemy item model"""
	__tablename__ = "items"

	# sql (relational) requirements
	id = Column(Integer, primary_key=True)

	type_id = Column(String, default=u'simple')
	attribute_set_id = Column(Integer, default=4)
	sku = Column(String)

class SQLManufacturer(DeclarativeBase):
	"""SQLAlchemy item model"""
	__tablename__ = "items"

	# sql (relational) requirements
	id = Column(Integer, primary_key=True)

	manufacturer = Column('manufacturer', String)
	manufacturer_logo = Column('manufacturer_logo', String)
