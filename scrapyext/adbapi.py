"""
AdbapiPipeline

"""

from scrapy import signals
from scrapy import log
from scrapy.conf import settings
from scrapy.exceptions import DropItem
from twisted.enterprise import adbapi

from project.items import MyItem as DatabaseItem

import datetime
import time
import MySQLdb
import MySQLdb.cursors


class AdbapiPipeline(object):

	sql_select = """SELECT id, sku, name FROM products WHERE sku = %s"""
	sql_create = """INSERT INTO item (`seller_idfk`, `batch_id`, `index`, `asin`, `title`, `quantity`, `cond`, `price`) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s )"""
	sql_create_item = """INSERT INTO products
		( `origin`, `ds`, `timestamp`, `type_id`, `attribute_set_id`, `sku`, `name`, `description`, `short_description`, `price`, `status`, `tax_class_id`, `visibility`, `weight`, `image_urls`, `supplier`, `supplier_url`, `supplier_category`, `pdf_link`, `msrp`, `unit_type`, `quantity`, `availability`, `deliverytime` ) VALUES
		( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"""
	sql_update = """UPDATE products SET spider=%s, name=%s WHERE url='%s'"""

	def __init__(self, settings, stats):
		if not isinstance(settings, dict):
			raise NotConfigured('No database connection settings found.')

		self.settings = settings

	@classmethod
	def from_crawler(cls, crawler):
		if not crawler.settings.get('DATABASE'):
			raise NotConfigured('No database connection settings found.')

		o = cls(settings=crawler.settings.get('DATABASE'), stats=crawler.stats)
		return o

	def open_spider(self, spider):
		self.connect()

	def connect(self):
		"""Connect to the database"""
		# PostgreSQL PyPgSQL
	#	cp = adbapi.ConnectionPool("pyPgSQL.PgSQL", database="test")
		# MySQL
		self.dbpool = adbapi.ConnectionPool('MySQLdb',
			host = self.settings.get('hostname', 'localhost'),
			port = self.settings.get('port', 3306),
			db = self.settings.get('database'),
			user = self.settings.get('username'),
			passwd = self.settings.get('password'),
			cursorclass = MySQLdb.cursors.DictCursor,
			charset = 'utf8',
			use_unicode = True,
		)

	def process_item(self, item, spider):
		"""Process items of type `DatabaseItem`."""
		if isinstance(item, DatabaseItem):
			# run db query in thread pool
			query = self.dbpool.runInteraction(self._conditional_op, item)
			query.addErrback(self._database_error, item)

		return item

	def _conditional_op(self, tx, item):
		"""Run transaction"""
		# primary key check
	#	tx.execute(self.sql_select, (item['sku']))
	#	result = tx.fetchone()
	#	if result:
	#		log.msg("Item already in db: (db id) %s (sku: %s) item:\n%s" % (result['id'], result['sku'], item), level=log.DEBUG)
	#		self.sid = result['id']
	#		return item

		# add record to db
		try:
			self._create_item(tx, item)
		except MySQLdb.IntegrityError:
			raise
	#	except MySQLdb.OperationalError, e:
	#		raise e
	#	except exceptions.TypeError: # wrong format, catch this
	#		raise

	def _database_error(self, e, item):
		"""Log exception"""
		log.err(e)

	# database operations

	def _create_item(self, tx, item):
		#log.msg("SQL Debug: %s" % self.sql_create_item, level=log.DEBUG)
		tx.execute(self.sql_create_item, (
			#	item.get('id'),
				item.get('origin'),
				item.get('ds', 0),
				item.get('timestamp'),
				item.get('type_id', 'simple'),
				item.get('attribute_set_id', 4),
				item.get('sku'),
				item.get('name'),
				item.get('description'),
				item.get('short_description'),
				item.get('price'),
				item.get('status', 1),
				item.get('tax_class_id', 2),
				item.get('visibility', 4),
				item.get('weight', 0),
				item.get('image_urls', None),
				item.get('supplier'),
				item.get('supplier_url'),
				item.get('supplier_category'),
				item.get('pdf_link'),
				item.get('msrp'),
				item.get('unit_type'),
				item.get('quantity'),
				item.get('availability'),
				item.get('deliverytime'),
			)
		)

		#log.msg("Item stored in db: %s" % item, level=log.DEBUG)

	def _update_item(self, item):
		# do update
		tx.execute(self.sql_update, (item['url'][0], int(time.time())) )

	def _delete_item(self, item):
		pass
