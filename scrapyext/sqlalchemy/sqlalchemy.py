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
    or
    DATABASE = 'mysql://root:toor@localhost/db'

"""

from scrapy.exceptions import NotConfigured, DropItem

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker, scoped_session
from .util.history_meta import versioned_session

from .models import create_db_tables
# SQLAlchemy models
from .models import SQLArticle, SQLManufacturer, SQLArticleImage
# respective scrapy items for verification
from .items import Article, Manufacturer, ArticleImage


class SQLAlchemyPipeline(object):
	"""Pipeline for storing scraped items in a database"""

	def __init__(self, settings, stats, **kwargs):
		"""Initializes database connection and sessionmaker.

		Creates database tables and session.
		"""
		if not settings.get('DATABASE'):
			raise NotConfigured
		db = settings.get('DATABASE')
		if isinstance(db, dict):
			db = URL(db)
		engine = create_engine(db)
	#	create_db_tables(engine)
		self.session = sessionmaker(bind=engine)
		#self.session = scoped_session(sessionmaker(bind=engine))
		versioned_session(self.session)

	@classmethod
	def from_crawler(cls, crawler):
		o = cls(crawler.settings, crawler.stats)
		o.crawler = crawler
		return o

	def process_item(self, item, spider):
		"""Save items in the database.

		This method is called for every item pipeline component.
		"""

		# mapping SQLAlchemy models to Items
		if isinstance(item, Article):
			sqlitem = SQLArticle(**item)
		elif isinstance(item, Manufacturer):
			sqlitem = SQLManufacturer(**item)
		elif isinstance(item, ArticleImage):
			sqlitem = SQLArticleImage(**item)
		else:
			return item

		session = self.session()

		try:
			session.add(sqlitem)
			session.commit()
		except:
			session.rollback()
			raise
		finally:
			session.close()

		return item
