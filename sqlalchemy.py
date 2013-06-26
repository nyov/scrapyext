from sqlalchemy.orm import sessionmaker
from sqlmodels import db_connect, create_db_table
### Database Model ###
from sqlmodels import Product

class SQLAlchemyPipeline(object):
	"""Pipeline for storing scraped items in a database"""

	def __init__(self):
		"""
		Initializes database connection and sessionmaker.

		Creates table.
		"""
		engine = db_connect()
	#	create_db_table(engine)
		self.Session = sessionmaker(bind=engine)

	def process_item(self, item, spider):
		"""
		Save items in the database.

		This method is called for every item pipeline component.
		"""
		session = self.Session()
		sqlitem = Product(**item)

		try:
			session.add(sqlitem)
			session.commit()
		except:
			session.rollback()
			raise
		finally:
			session.close()

		return item
