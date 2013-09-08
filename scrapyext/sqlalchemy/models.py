"""
SQLAlchemy models - defines table for storing scraped data.

"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey, Column, Integer, Float, String, Text, DateTime
from sqlalchemy.orm import relationship
from .util.history_meta import Versioned


Base = declarative_base()

def create_db_tables(engine):
	"""Create database tables"""
	Base.metadata.create_all(engine)

# SQLAlchemy models
class SQLItem(Versioned, Base):
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

	def __eq__(self, other):
		assert type(other) is SQLItem and other.id == self.id

class SQLArticle(Versioned, Base):
	"""SQLAlchemy item model"""
	__tablename__ = "article"

	# sql (relational) requirements
	id = Column(Integer, primary_key=True)
	manufacturer_id = Column(Integer, ForeignKey('manufacturer.id'))
	manufacturer = relationship("SQLManufacturer", backref='parents')

	type_id = Column(String, default=u'simple')
	attribute_set_id = Column(Integer, default=4)
	sku = Column(String)

	def __eq__(self, other):
		assert type(other) is SQLArticle and other.id == self.id

class SQLManufacturer(Versioned, Base):
	"""SQLAlchemy item model"""
	__tablename__ = "manufacturer"

	# sql (relational) requirements
	id = Column(Integer, primary_key=True)

	manufacturer = Column('manufacturer', String)
	manufacturer_logo = Column('manufacturer_logo', String)

	def __eq__(self, other):
		assert type(other) is SQLManufacturer and other.id == self.id
