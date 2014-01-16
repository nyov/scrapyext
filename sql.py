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

"""
from scrapy import log
from scrapy import signals
from scrapy.item import Item, Field
from scrapy.exceptions import NotConfigured, DropItem

from twisted.enterprise.adbapi import ConnectionPool

from collections import OrderedDict
import re


class UniqueField(Field):
	"""Field to tell SQLPipeline about an index.
	"""

class SQLItem(Item):
	"""Item to support database operations in SQLPipeline.
	"""
	__tablename__ = ''


_ENTITIES_RE = re.compile(r'(\$[a-z_]+)(:[\w,]+)?')

class SQLPipeline(object):

	def __init__(self, settings, **kwargs):
		"""Connect to database in the pool."""

		if not isinstance(settings, dict):
			raise NotConfigured('No database connection settings found.')

		self.settings = settings
		self.paramstyle = ':'

		if self.settings.get('drivername') == 'sqlite3' or self.settings.get('drivername') == 'sqlite':
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
		elif self.settings.get('drivername') == 'pgsql' or self.settings.get('drivername') == 'postgres':
			self.__dbpool = ConnectionPool('pyPgSQL.PgSQL', database=self.settings.get('database'),
				user = self.settings.get('username'),
				password = self.settings.get('password', None),
				host = self.settings.get('hostname', None), # default to unix socket
				port = self.settings.get('port', '5432'),
			#	cursor_factory = None,
			)
			#self.paramstyle = '?'
			#self.paramstyle = '%s'

		#	import psycopg2
		#	self.__dbpool = ConnectionPool('psycopg2', database=self.settings.get('database'),
		#		user = self.settings.get('username'),
		#		password = self.settings.get('password', None),
		#		host = self.settings.get('hostname', None), # should default to unix socket
		#		port = self.settings.get('port', '5432'),
		#	#	cursor_factory = psycopg2.extras.DictCursor,
		#	)
		elif self.settings.get('drivername') == 'mysql':
			import MySQLdb
			from MySQLdb import cursors
			self.__dbpool = ConnectionPool('MySQLdb', db=self.settings.get('database'),
				user = self.settings.get('username'),
				passwd = self.settings.get('password', None),
				host = self.settings.get('hostname', 'localhost'), # should default to unix socket
				port = self.settings.get('port', 3306),
				cursorclass = cursors.DictCursor,
				charset = 'utf8',
				use_unicode = True,
				# connpool settings
				cp_reconnect = True,
			)
			self.paramstyle = '%s'

		self.queries = kwargs.get('queries')

	@classmethod
	def from_crawler(cls, crawler):
		if not crawler.settings.get('DATABASE'):
			raise NotConfigured('No database connection settings found.')

		# Prepared statements using magic variables
		# supported fields:
		#
		# $table
		#	item database tablename
		#	will be replaced by SQLItem's __tablename__ or item class name
		#
		#	$table:escaped
		#	- tablename will be escaped (default)
		# $fields (table cells)
		#	item fields (keys), escaped
		#	will be replaced by fields found in item
		#	- concatenated by comma (,)
		#	(equals `$fields:,` )
		#
		#	$fields:and
		#	- concatenated by ' AND '
		# $values
		#	item field's scraped contents
		#	will be replaced by items' $fields respective values
		#	- concatenated by comma (,)
		# $fields_values
		#	item $fields and their respective $values (as above),
		#	joined by '='. ($field1=$value1, $field2=$value2, ...)
		#	- concatenated by comma (,)
		#
		#	$fields_values:and
		#	- concatenated by ' AND '
		# $indices
		# $indexes
		#	item's unique field(s) if defined
		#	- concatenated by ','
		#
		#	$indexes:and (THIS IS USUALLY WHAT YOU WANT in the "WHERE" clause)
		#	- concatenated by ' AND '
		#	  (This means all UniqueFields are counted as a single constraint)
		#
		SQL_QUERIES = [
			('select', "SELECT $fields FROM $table:esc"),
			('insert', "INSERT INTO $table:esc ($fields) VALUES ($values)"), #! mysql
		#	('insert', "INSERT INTO $table:esc SET $fields_values"), #! sqlite
		#	('upsert', "INSERT OR REPLACE INTO $table:esc ($fields) VALUES ($values)"), #! sqlite
			('upsert', "REPLACE INTO $table ($fields) VALUES ($values)"), #! mysql
		#	('upsert', "INSERT INTO $table:esc SET $fields_values ON DUPLICATE KEY UPDATE $fields_values"),
			('deleteme', "DELETE FROM $table:esc WHERE $fields_values:and"), # exact item match required
			# have indices?
			('update', "UPDATE $table:esc SET $fields_values WHERE $indices:and"),
			('fetchall', "SELECT $fields FROM $table:esc WHERE $indices:and"), # select on unique keys
			('fetchone', "SELECT $fields FROM $table:esc WHERE $indices:and LIMIT 1"), # if backend supports LIMIT
			('delete', "DELETE FROM $table:esc WHERE $indices:and"), # match on unique fields
			# ... (add your own)
		]
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

		# Only process items inheriting SQLItem
		if not isinstance(item, SQLItem):
			return item

		query, values = self.sql_for(item, 'upsert')

		deferred = self.operation((query[0], values), item, spider)
		deferred.addErrback(self._database_error, item, spider)

		# always return item
		deferred.addBoth(lambda _: item)

		return deferred

	def operation(self, op, item, spider):
		def onsuccess(result, query, params):
			for p in params: # possibly inaccurate if param order switches.
				query = re.sub('(\\'+self.paramstyle+r'\d?)', '"%s"' % p, query, count=1)
			log.msg('%s executed: %s' % (self.__class__.__name__, query), level=log.DEBUG, spider=spider)

		query, params = op

		deferred = self.__dbpool.runOperation(query, params)
		deferred.addCallback(onsuccess, query, params)
		deferred.addErrback(self._database_error, item, spider)
		return deferred

	def sql_for(self, item, querytype=None):
		"""Prepared statements for Item

		(notice, not every dbapi driver does actually prepare
		 statements in database, only escapes them (e.g. MySQLdb))
		"""
		def paramstyle(value):
			if not isinstance(value, int):
				raise ValueError('Integer required.')
			if self.paramstyle == '?' or self.paramstyle == '%s':
				return self.paramstyle
			else:
				return self.paramstyle + str(value)

		tablename = item.__tablename__ or item.__class__.__name__ #.lower().rstrip('item')

		indices = []
		for n, v in item.fields.iteritems():
			if isinstance(v, UniqueField):
				indices += [n]

		# convert to preserve item order from here on out
		item = OrderedDict(item)

		# if this seems to be a destructive operation, check for missing indices
		if querytype.lower() not in ('select'):
			for val in (v for v in indices if v not in item.keys()):
				raise ValueError('Item missing value for UniqueField "%s" but index must be present.' % val)
				#raise DropItem('Item missing value for UniqueField "%s" but index must be present.' % val)

		# order indices
		i = []
		for key in (k for k in item.keys() if k in indices):
			i.append(key)
		indices = i

		itemkeys   = ['`%s`' % f for f in indices] # escaping
		itemfields = ['`%s`' % f for f in item.keys()] # escaping

		# return this for matching order with prepared statement on execution
		itemvalues = []

		prepared_statements = []
		for query, statement in self.queries:
			if querytype and query != querytype:
				continue
			final = statement
			for m in _ENTITIES_RE.finditer(statement):
				entity, args = m.groups()
				args = (args or ':')[1:]
				field = value = None
				if entity == '$table':
					field = tablename
					if args[:3] == 'esc':
						field = '`%s`' % tablename
				elif entity == '$fields':
					attr = ','
					if args:
						attr = ' %s ' % args.upper()
					field = attr.join(itemfields)
				elif entity == '$values':
					attr = ','
					if args:
						attr = ' %s ' % args.upper()
					field = attr.join([paramstyle(i) for i, _ in enumerate(itemfields)])
					# FIXME: handle this better
					value = item.values()
				elif entity == '$fields_values':
					attr = ','
					if args:
						attr = ' %s ' % args.upper()
					values = ['%s=%s' % (k, paramstyle(i)) for i, k in enumerate(itemfields)]
					field = attr.join(values)
					# FIXME: handle this better
					value = item.values()
				elif entity == '$indices' or entity == '$indexes':
					attr = ','
					if args:
						attr = ' %s ' % args.upper()
					indexes = ['%s=%s' % (idx, paramstyle(i)) for i, idx in enumerate(itemkeys)]
					field = attr.join(indexes)
					# FIXME: handle this better
					value = [v for k, v in item.iteritems() if k in indices]
				elif entity:
					raise ValueError('Invalid entity in SQL_QUERIES: "%s"' % entity)

				if field is not None:
					final = final.replace(m.group(), field, 1)
					if value is not None:
						itemvalues.extend(value)

			if querytype:
				prepared_statements.append(final)
			else:
				prepared_statements.append((query, final))

		return prepared_statements, itemvalues

	def _database_error(self, e, item, spider=None):
		"""Log exceptions."""
		if spider:
			log.err(e, spider=spider)
		else:
			log.err(e)

	def query(self, sql):
		deferred = self.__dbpool.runQuery(sql)
		deferred.addErrback(log.err)
		return deferred
