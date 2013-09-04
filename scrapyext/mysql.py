"""
MysqlDBPipeline

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


class MysqlDBPipeline(object):

	sql_select = """SELECT id, sku, name FROM products WHERE sku = %s"""
	sql_create = """INSERT INTO item (`seller_idfk`, `batch_id`, `index`, `asin`, `title`, `quantity`, `cond`, `price`) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s )"""
	sql_create_item = """INSERT INTO products
		( `origin`, `ds`, `timestamp`, `type_id`, `attribute_set_id`, `sku`, `name`, `description`, `short_description`, `price`, `status`, `tax_class_id`, `visibility`, `weight`, `image_urls`, `supplier`, `supplier_url`, `supplier_category`, `pdf_link`, `msrp`, `unit_type`, `quantity`, `availability`, `deliverytime` ) VALUES
		( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"""
	sql_update = ""

	def __init__(self):
		"""
		Connect to the database in the pool.
		"""
		# PostgreSQL PyPgSQL
	#	cp = adbapi.ConnectionPool("pyPgSQL.PgSQL", database="test")
		# MySQL
	#	cp = adbapi.ConnectionPool("MySQLdb", db="test")
		self.dbpool = adbapi.ConnectionPool('MySQLdb',
			#settings['MYSQLDB_SERVER'] or "localhost",
			#settings['MYSQLDB_PORT'] or 3306,
			db = settings['MYSQLDB_DB'],
			user = settings['MYSQLDB_USER'],
			passwd = settings['MYSQLDB_PASS'],
			cursorclass = MySQLdb.cursors.DictCursor,
			charset = 'utf8',
			use_unicode = True,
		)

	def process_item(self, item, spider):
		"""
		Run db query in thread pool and call :func:`_conditional_insert`.
		We only want to process Items of type `DatabaseItem`.
		"""
		if isinstance(item, DatabaseItem):
			# run db query in thread pool
			query = self.dbpool.runInteraction(self._conditional_op, item)
			query.addErrback(self._database_error, item)

		return item

	def _conditional_op(self, tx, item):
		"""
		Insert an entry in the `log` table and update the `seller` table,
		if necessary, with the seller's name.
		"""
		# primary key check
	#	tx.execute(self.sql_select, (item['sku']))
	#	result = tx.fetchone()
	#	if result:
	#		log.msg("Item already in db: (db id) %s (sku: %s) item:\n%s" % (result['id'], result['sku'], item), level=log.DEBUG)
	#		self.sid = result['id']
	#		return
	# fast fail - do not query but silently ignore duplicate key errors (querys eat too much into write performance)

		# add record to db
		try:
			self._create_item(tx, item)
		except MySQLdb.IntegrityError:
			pass
	#		raise
	#	except MySQLdb.OperationalError, e:
	#		raise e
	#	except exceptions.TypeError: # wrong format, catch this

	def _database_error(self, e, item):
		"""
		Log an exception to the Scrapy log buffer.
		"""
		log.err(e)

	# database operations

	def _create_item(self, tx, item):
		#log.msg("SQL Debug: %s" % self.sql_create_item, level=log.DEBUG)
		tx.execute(
			self.sql_create_item,
			(
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
		#tx.execute(\
		#	 "UPDATE products SET "
		#	 "spider=%s, "
		#	 "name=%s "
		#	 "WHERE url = '%s'",
		#	 (item['url'][0], int(time.time()))
		#)
		pass

	def _delete_item(self, item):
		pass
