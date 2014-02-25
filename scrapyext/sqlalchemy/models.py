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

	def __repr__(self):
		return '<Item(id=%r)>' % (self.id)


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
	name = Column(String(255))

	def __eq__(self, other):
		assert type(other) is SQLArticle and other.id == self.id

	def __repr__(self):
		return '<Article(id=%r, sku=%r, name=%r)>' % (self.id, self.sku, self.name)


class SQLManufacturer(Versioned, Base):
	"""SQLAlchemy item model"""
	__tablename__ = "manufacturer"

	# sql (relational) requirements
	id = Column(Integer, primary_key=True)

	manufacturer = Column(String)
	manufacturer_logo = Column(String)

	def __eq__(self, other):
		assert type(other) is SQLManufacturer and other.id == self.id

	def __repr__(self):
		return '<Manufacturer(id=%r, manufacturer=%r)>' % (self.id, self.manufacturer)


class SQLArticleImage(Versioned, Base):
	"""SQLAlchemy item model"""
	__tablename__ = "article_image"

	# sql (relational) requirements
	id = Column(Integer, primary_key=True)

	def __eq__(self, other):
		assert type(other) is SQLArticleImage and other.id == self.id

	def __repr__(self):
		return '<ArticleImage(id=%r)>' % (self.id)
