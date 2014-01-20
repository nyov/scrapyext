"""
SQLPipeline

The SQLPipeline uses twisted adbapi connection pools
to put items into an RDBMS.

Define items for SQLPipeline in your items.py:

```
from .pipelines.sql import SQLItem, UniqueField

class MyItem(SQLItem):
	__tablename__ = 'my_items'

	# sql unique key field:
	id = UniqueField()
	# normal field:
	spider = Field()
```

Prepared statements using magic variables:

 $table
	item database tablename
	will be replaced by SQLItem's __tablename__ or item class name

	$table:escaped
	- tablename will be escaped (default)
 $fields (table cells)
	item fields (keys), escaped
	will be replaced by fields found in item
	- concatenated by comma (,)
	(equals `$fields:,` )

	$fields:and
	- concatenated by ' AND '
 $values
	item field's scraped contents
	will be replaced by items' $fields respective values
	- concatenated by comma (,)
 $fields_values
	item $fields and their respective $values (as above),
	joined by '='. ($field1=$value1, $field2=$value2, ...)
	- concatenated by comma (,)

	$fields_values:and
	- concatenated by ' AND '
 $indices
 $indexes
	item's unique field(s) if defined
	- concatenated by ','

	$indexes:and (THIS IS USUALLY WHAT YOU WANT in the "WHERE" clause)
	- concatenated by ' AND '
	  (This means all UniqueFields are counted as a single constraint)

"""
from scrapy import log
from scrapy import signals
from scrapy.item import Item, Field
from scrapy.exceptions import NotConfigured, DropItem

from twisted.enterprise.adbapi import ConnectionPool

from collections import OrderedDict
import re


_ENTITIES_RE = re.compile(r'(\$[a-z_]+)(:[\w,]+)?')

def _sql_format(query, item, paramstyle=':', identifier='"'):
	"""Prepared statements for Item

	(Notice, not every dbapi driver does actually prepare
	 statements in-database, only escapes them beforehand
	 (e.g. MySQLdb))
	"""
	def paramformat(value):
		# simple or numbered params?
		if not isinstance(value, int):
			raise ValueError('Integer required.')
		if paramstyle == '?' or paramstyle == '%s':
			return paramstyle
		else:
			return paramstyle + str(value)

	tablename = item.__tablename__ or item.__class__.__name__ #.lower().rstrip('item')

	indices = []
	for n, v in item.fields.iteritems():
		if isinstance(v, UniqueField):
			indices += [n]

	# convert to preserve item order from here on out
	item = OrderedDict(item)

	# if this seems to be a destructive operation, check for missing indices
	if query[:6].lower() not in ('select'):
		for val in (v for v in indices if v not in item.keys()):
			raise ValueError('Item missing value for UniqueField "%s" but index must be present.' % val)

	# order indices
	i = []
	for key in (k for k in item.keys() if k in indices):
		i.append(key)
	indices = i

	itemkeys   = ['{0}{1}{0}'.format(identifier, f) for f in indices] # escaping
	itemfields = ['{0}{1}{0}'.format(identifier, f) for f in item.keys()] # escaping

	itemvalues = []

	prepared = query
	for m in _ENTITIES_RE.finditer(query):
		entity, args = m.groups()
		args = (args or ':')[1:]
		field = value = None
		if entity == '$table':
			field = tablename
			if args[:3] == 'esc':
				field = '{0}{1}{0}'.format(identifier, tablename)
		elif entity == '$fields':
			attr = ','
			if args:
				attr = ' %s ' % args.upper()
			field = attr.join(itemfields)
		elif entity == '$values':
			attr = ','
			if args:
				attr = ' %s ' % args.upper()
			field = attr.join([paramformat(i) for i, _ in enumerate(itemfields)])
			# FIXME: handle this better?
			value = item.values()
		elif entity == '$fields_values':
			attr = ','
			if args:
				attr = ' %s ' % args.upper()
			values = ['%s=%s' % (k, paramformat(i)) for i, k in enumerate(itemfields)]
			field = attr.join(values)
			# FIXME: handle this better?
			value = item.values()
		elif entity == '$indices' or entity == '$indexes':
			attr = ','
			if args:
				attr = ' %s ' % args.upper()
			indexes = ['%s=%s' % (idx, paramformat(i)) for i, idx in enumerate(itemkeys)]
			field = attr.join(indexes)
			# FIXME: handle this better?
			value = [v for k, v in item.iteritems() if k in indices]
		elif entity:
			raise ValueError('Invalid entity in SQL_QUERIES: "%s"' % entity)

		if field is not None:
			prepared = prepared.replace(m.group(), field, 1)
			if value is not None:
				itemvalues.extend(value)

	return prepared, itemvalues


class UniqueField(Field):
	"""Field to tell SQLPipeline about an index.
	"""

class SQLItem(Item):
	"""Item to support database operations in SQLPipeline.
	"""
	__tablename__ = ''


class SQLPipeline(object):

	def __init__(self, settings, **kwargs):
		"""Connect to database in the pool."""

		if not isinstance(settings, dict):
			raise NotConfigured('No database connection settings found.')

		self.settings = settings
		self.paramstyle = ':'
		self.identifier = '"' # ANSI

		if self.settings.get('drivername') == 'sqlite':
			self.__dbpool = ConnectionPool('sqlite3', self.settings.get('database', ':memory:'),
				# apparently the connection pool / thread pool does not do the teardown in the same thread
				# https://twistedmatrix.com/trac/ticket/3629
				# therefore throwing errors on finalClose at reactor shutdown
				# TODO: should be able to work around that?
				check_same_thread=False, # SQLite must be compiled threadsafe to use this
				# limit connection pool to one thread to avoid "database is locked" errors
				#cp_max=1,
				# - or raise the database timeout sufficiently
				timeout=300,
			)
			# alternative escaping parameter
			#self.paramstyle = '?'
			#self.paramstyle = ':'
			#self.paramstyle = '$'
		elif self.settings.get('drivername') == 'pgsql':
			#from psycopg2.extras import DictCursor
			self.__dbpool = ConnectionPool('psycopg2', database=self.settings.get('database'),
				user = self.settings.get('username'),
				password = self.settings.get('password', None),
				host = self.settings.get('host', None), # default to unix socket
				port = self.settings.get('port', '5432'),
			#	cursor_factory = DictCursor,
			)
			self.paramstyle = '%s'

		elif self.settings.get('drivername') == 'mysql':
			#import MySQLdb
			from MySQLdb import cursors
			self.__dbpool = ConnectionPool('MySQLdb', db=self.settings.get('database'),
				user = self.settings.get('username'),
				passwd = self.settings.get('password', None),
				host = self.settings.get('host', 'localhost'), # should default to unix socket
				port = self.settings.get('port', 3306),
				cursorclass = cursors.DictCursor,
				charset = 'utf8',
				use_unicode = True,
				# connpool settings
				cp_reconnect = True,
			)
			self.paramstyle = '%s'
			self.identifier = '`' # MySQL

		self.queries = kwargs.get('queries')

	@classmethod
	def from_crawler(cls, crawler):
		if not crawler.settings.get('DATABASE'):
			raise NotConfigured('No database connection settings found.')

		SQL_QUERIES = {
			'select': "SELECT $fields FROM $table:esc",
			'insert': "INSERT INTO $table:esc ($fields) VALUES ($values)",
		#	'insert': "INSERT INTO $table:esc SET $fields_values", #! sqlite
		#	'upsert': "INSERT OR REPLACE INTO $table:esc ($fields) VALUES ($values)", #! sqlite
			'upsert': "REPLACE INTO $table ($fields) VALUES ($values)", #! mysql
		#	'upsert': "INSERT INTO $table:esc SET $fields_values ON DUPLICATE KEY UPDATE $fields_values",
			'deleteme': "DELETE FROM $table:esc WHERE $fields_values:and", # exact item match required
			# have indices?
			'update': "UPDATE $table:esc SET $fields_values WHERE $indices:and",
			'fetchall': "SELECT $fields FROM $table:esc WHERE $indices:and", # select on unique keys
			'fetchone': "SELECT $fields FROM $table:esc WHERE $indices:and LIMIT 1", # if backend supports LIMIT
			'delete': "DELETE FROM $table:esc WHERE $indices:and", # match on unique fields
			# ... (add your own)
		}
		# crawler.settings.get('SQL_QUERIES')

		o = cls(settings=crawler.settings.get('DATABASE'), stats=crawler.stats, queries=SQL_QUERIES)
		return o

	def open_spider(self, spider):
		self.on_connect()

	def on_connect(self):
		## override this to run some queries after connecting
		# e.g. create tables for an in-memory SQLite database
		pass

	def close_spider(self, spider):
		self.shutdown()

	def shutdown(self):
		"""Shutdown connection pool, kill associated threads"""
		self.__dbpool.close()

	def process_item(self, item, spider):
		"""Process the item."""

		def onerror(error, query, params):
			for p in params:
				query = re.sub('(\\'+self.paramstyle+r'\d?)', '"%s"' % p, query, count=1)
			log.msg('%s failed executing: %s' % (self.__class__.__name__, query), level=log.ERROR, spider=spider)
			raise error
		def onsuccess(result, query, params):
			for p in params:
				query = re.sub('(\\'+self.paramstyle+r'\d?)', '"%s"' % p, query, count=1)
			log.msg('%s executed: %s' % (self.__class__.__name__, query), level=log.DEBUG, spider=spider)

		# Only process items inheriting SQLItem
		if not isinstance(item, SQLItem):
			return item

	#	query, params = _sql_format(self.queries['upsert'], item, paramstyle=self.paramstyle, identifier=self.identifier)
		query, params = _sql_format(self.queries['insert'], item, paramstyle=self.paramstyle, identifier=self.identifier)
		update, uvalues = _sql_format(self.queries['update'], item, paramstyle=self.paramstyle, identifier=self.identifier)

	#	deferred = self.operation((query, params), item, spider)
	#	deferred.addCallback(onsuccess, query, params)
	#	deferred.addErrback(onerror, query, params)
	#	deferred.addErrback(self._database_error, item, spider)

		deferred = self.insert_update((query, params), (update, uvalues), item, spider)
		deferred.addCallback(onsuccess, query, params)
		#deferred.addErrback(onerror, query, params)
		deferred.addErrback(onerror, update, uvalues)
		deferred.addErrback(self._database_error, item, spider)

		# always return item
		deferred.addBoth(lambda _: item)

		return deferred

		'''
		deferred = self.__dbpool.runInteraction(self.transaction, item, spider)
	#	deferred.addCallback(onsuccess, query, params)
	#	deferred.addErrback(onerror, query, params)
		deferred.addErrback(self._database_error, item, spider)

		# always return item
		deferred.addBoth(lambda _: item)

		return deferred
		'''

	def operation(self, query, params, item, spider):
		deferred = self.__dbpool.runOperation(query, params)
		return deferred

	def insert_update(self, insert, update, item, spider):
		import MySQLdb
		def upsert(result, query, params):
			result.trap(MySQLdb.IntegrityError)
			spider.log('%s executing: %s' % (self.__class__.__name__, query), level=log.ERROR)
			deferred = self.__dbpool.runOperation(query, params)
			return deferred

		query, params = insert
		uquery, uparams = update
		deferred = self.__dbpool.runOperation(query, params)
		deferred.addErrback(upsert, uquery, uparams)
		return deferred

	def transaction(self, txn, item, spider):
		# This will run in a thread, we can use blocking calls

		import MySQLdb

		# run INSERT, UPDATE on failure
		# (mysql wont error trying to update nonexistant column :()
		try:
			query, params = _sql_format(self.queries['insert'], item, paramstyle=self.paramstyle, identifier=self.identifier)
			spider.log("SQL: %s | %s" % (query, params), level=log.DEBUG)
			txn.execute(query, params)
		#except sqlite3.IntegrityError:
		except MySQLdb.IntegrityError:
			query, params = _sql_format(self.queries['update'], item, paramstyle=self.paramstyle, identifier=self.identifier)
			spider.log("SQL: %s | %s" % (query, params), level=log.DEBUG)
			txn.execute(query, params)

		# primary key check
	#	tx.execute(query, (item['url']))
	#	result = tx.fetchone()
	#	if result:
	#		log.msg("Item already in db: (id) %s item:\n%r" % (result['id'], item), level=log.DEBUG)

	def _database_error(self, e, item, spider=None):
		"""Log exceptions."""
		if spider:
			log.err(e, spider=spider)
		else:
			log.err(e)

	def query(self, sql):
		deferred = self.__dbpool.runQuery(sql)
		return deferred
