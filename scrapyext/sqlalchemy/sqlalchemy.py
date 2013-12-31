"""
SQLAlchemyPipeline

SQLAlchemy pipeline for storing scraped items in the database
Settings:

    ITEM_PIPELINES = ['project.pipelines.SQLAlchemyPipeline']

    DATABASE = {
        'drivername': 'postgres',
        'host': 'localhost',
        'port': '5432',
        'username': 'root',
        'password': 'root',
        'database': 'test'
    }

"""

from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker

from .models import create_db_table
from .models import SQLItem

def db_connect(settings):
	"""Database connection using database settings from settings.py.
	Returns sqlalchemy engine instance
	"""
	return create_engine(URL(**settings.DATABASE))


class SQLAlchemyPipeline(object):
	"""Pipeline for storing scraped items in a database"""

	def __init__(self, settings):
		"""Initializes database connection and sessionmaker.

		Creates table.
		"""
		engine = db_connect(settings)
	#	create_db_table(engine)
		self.session = sessionmaker(bind=engine)

	def process_item(self, item, spider):
		"""Save items in the database.

		This method is called for every item pipeline component.
		"""
		session = self.session()
		sqlitem = SQLItem(**item)

		try:
			session.add(sqlitem)
			session.commit()
		except:
			session.rollback()
			raise
		finally:
			session.close()

		return item
