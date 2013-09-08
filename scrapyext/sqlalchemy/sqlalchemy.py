"""
SQLAlchemyPipeline

Pipeline for storing scraped items in the database
Settings:

    ITEM_PIPELINES = ['project.pipelines.SQLAlchemyPipeline']

    DATABASE = {
        'drivername': 'postgres',
        'host': 'localhost',
        'port': 5432,
        'database': 'test',
        'username': 'root',
        'password': 'root',
    }

"""

from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from .util.history_meta import versioned_session

from .models import create_db_tables
# SQLAlchemy models
from .models import SQLArticle, SQLManufacturer

def db_connect(settings):
	"""Database connection using database settings from settings.py.

	Returns sqlalchemy engine instance.
	"""
	return create_engine(URL(**settings.DATABASE))


class SQLAlchemyPipeline(object):
	"""Pipeline for storing scraped items in a database"""

	def __init__(self, settings):
		"""Initializes database connection and sessionmaker.

		Creates database tables and session.
		"""
		engine = db_connect(settings)
	#	create_db_tables(engine)
		self.session = sessionmaker(bind=engine)
		versioned_session(self.session)

	def process_item(self, item, spider):
		"""Save items in the database.

		This method is called for every item pipeline component.
		"""
		session = self.session()
		# our Item name
		sqlitem = SQLArticle(**item)
		# case switch here?
	#	sqlitem = SQLManufacturer(**item)

		try:
			session.add(sqlitem)
			session.commit()
		except:
			session.rollback()
			raise
		finally:
			session.close()

		return item
