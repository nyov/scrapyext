"""
SQLMagicPipeline

SQLMagicPipeline uses twisted adbapi connection pools
to put items into an RDBMS.
It uses "magic variables" in "prepared SQL statements"
(as supported by the underlying dbapi driver) to evaluate
simple SQL statements, without adding the complexities
and pitfalls of an ORM.

(WIP. CURRENTLY ONLY RUNS DEFINED 'insert' and 'update' QUERIES.)

    ITEM_PIPELINES = [
        'project.pipelines.sqlmagic.SQLMagicPipeline',
    ]

Settings:

    SQLMAGIC_DATABASE = { # (dict)
        'drivername': 'mysql', # (required)
        'database': 'db', # (required)
        'username': 'user',
        'password': 'pass',
        'host': 'localhost',
        'port': 3306,
    }
    SQLMAGIC_QUERIES = {  # (dict)
        # (optional. usual defaults per database included in code)
        'insert': "INSERT INTO $table:esc ($fields) VALUES ($values)",
        'update': "UPDATE $table:esc SET $fields_values WHERE $indices:and",
    }
    # log prepared sql queries and operational errors for debugging
    SQLMAGIC_DEBUG = True # (bool)

Define items for SQLPipeline in your items.py as such:

```
from .pipelines.sqlmagic import SQLItem, UniqueField

class MyItem(SQLItem):
	__tablename__ = 'my_items'

	# sql unique key field:
	id = UniqueField()
	# normal field:
	spider = Field()
```

SQLMagicPipeline will only process SQLItem types, for safety reasons.
Standard items are ignored by this pipeline (and can be safely mixed).

A `UniqueField` denotes a field which is unique.
It may be a PRIMARY KEY or UNIQUE key constraint in the database,
or only emulate one,
and will be filled in any `$indexes` magic field.
Multiple `UniqueField`s will convert to a single constraint
("...WHERE x=? AND y=?...").


Magic variables for building SQL queries:

 $table
	item database tablename
	will be replaced by SQLItem's __tablename__ attribute
	(or the item's class name, if missing)

	$table:escaped
	- tablename will be quoted/escaped
 $fields (table cells)
	item fields (keys), quoted/escaped
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
 $fields2values
	item $fields and their respective $values (as above),
	joined by '='. ($field1=$value1, $field2=$value2, ...)
	- concatenated by comma (,)

	$fields_values:and
	- concatenated by ' AND '
 $indices
 $indexes
	item's unique field(s) if defined
	- concatenated by ','
	(equals `$indices:,` )

	$indexes:and (THIS IS USUALLY WHAT YOU WANT in the "WHERE" clause)
	- concatenated by ' AND '
	  (This means all UniqueFields are counted as a single constraint)


Example illustration:

Suppose an SQLItem `Book' with the fields 'isbn', 'title' and 'description':
The isbn would be the unique field, used to detect duplicates or do updates.

class Book(SQLItem):
	__tablename__ = 'book'
	isbn = UniqueField()
	title = Field()
	description = Field()

Your query template for this Item, using a MySQL database, could look like this:

"INSERT INTO $table SET $fields_values ON DUPLICATE KEY UPDATE $fields_values"

which SQLPipeline would evaluate into:

"INSERT INTO `book` SET `isbn`=?,`title`=?,`description`=?
	ON DUPLICATE KEY UPDATE `isbn`=?,`title`=?,`description`=?"

using any given values from the current scraped item (and MySQL quoting).
(This requires unique/primary keys in the database table.)

Field values should match their database equivalent in python
to avoid surprises (int as int, string as string, etc.),
or may be converted by the dbapi driver.

Another example:

"UPDATE $table SET $fields_values WHERE $indices:and"

would get expanded to

"UPDATE `book` SET `isbn`=?,`title`=?,`description`=? WHERE `isbn`=?"

"""
from scrapy import log
from scrapy import signals
from scrapy.item import Item, Field
from scrapy.exceptions import NotConfigured, DropItem

from twisted.enterprise.adbapi import ConnectionPool

from collections import OrderedDict
import re


import MySQLdb

class ReconnectingConnectionPool(ConnectionPool):
	"""Reconnecting adbapi connection pool for MySQL.

	This class improves on the solution posted at
	http://www.gelens.org/2008/09/12/reinitializing-twisted-connectionpool/
	by checking exceptions by error code and only disconnecting the current
	connection instead of all of them.

	Also see:
	http://twistedmatrix.com/pipermail/twisted-python/2009-July/020007.html

	"""
	def _runInteraction(self, interaction, *args, **kw):
		try:
			return ConnectionPool._runInteraction(self, interaction, *args, **kw)
		except MySQLdb.OperationalError as e:
			if e[0] not in (2006, 2013, 1213):
				raise
			# 2006 MySQL server has gone away
			# 2013 Lost connection to MySQL server
			# 1213 Deadlock found when trying to get lock; try restarting transaction
			log.msg("%s got error %s, retrying operation" % (self.__class__.__name__, e))
			conn = self.connections.get(self.threadID())
			self.disconnect(conn)
			# try the interaction again
			return ConnectionPool._runInteraction(self, interaction, *args, **kw)
		except MySQLdb.InterfaceError as e:
			if e[0] not in (0,):
				raise
			# 0 Interface error (conn gone away or closed)
			log.msg("%s got error %s, retrying operation" % (self.__class__.__name__, e))
			conn = self.connections.get(self.threadID())
			self.disconnect(conn)
			# try the interaction again
			return ConnectionPool._runInteraction(self, interaction, *args, **kw)


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
			raise ValueError('Item missing value for UniqueField "%s" but index must be present. Item is:\n%r' % (val, dict(item)))

	# order indices
	i = []
	for key in (k for k in item.keys() if k in indices):
		i.append(key)
	indices = i

	itemkeys   = ['{0}{1}{0}'.format(identifier, f) for f in indices] # quoting
	itemfields = ['{0}{1}{0}'.format(identifier, f) for f in item.keys()] # quoting

	itemvalues = []

	prepared = query
	for m in _ENTITIES_RE.finditer(query):
		entity, args = m.groups()
		args = (args or ':')[1:]
		field = None
		value = None
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
		elif entity == '$fields_values' or entity == '$fields2values':
			attr = ','
			if args:
				attr = ' %s ' % args.upper()
			values = ['%s=%s' % (k, paramformat(i)) for i, k in enumerate(itemfields)]
			field = attr.join(values)
			# FIXME: handle this better?
			value = item.values()
		elif entity == '$indices' or entity == '$indexes':
			# TODO: should add true (1=1), if no indices exist
			attr = ','
			if args:
				attr = ' %s ' % args.upper()
			indexes = ['%s=%s' % (idx, paramformat(i)) for i, idx in enumerate(itemkeys)]
			field = attr.join(indexes)
			# FIXME: handle this better?
			value = [v for k, v in item.iteritems() if k in indices]
		elif entity:
			raise ValueError('Invalid entity for magic field: "%s"' % entity)

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


class SQLMagicPipeline(object):

	def __init__(self, settings, **kwargs):
		"""Connect to database in the pool."""

		if not isinstance(settings, dict):
			raise NotConfigured('No database connection settings found.')

		self.settings = settings
		self.stats = kwargs.get('stats')
		self.debug = kwargs.get('debug', False)
		self.paramstyle = ':'
		self.identifier = '"' # ANSI quoting
		self.queries = {
			'select': "SELECT $fields FROM $table:esc WHERE $indices:and", # select on UniqueFields
			'selectall': "SELECT $fields FROM $table:esc",
			'selectone': "SELECT $fields FROM $table:esc WHERE $indices:and LIMIT 1", # if backend supports LIMIT
			#
			'delete'  : "DELETE FROM $table:esc WHERE $indices:and", # match on UniqueFields
			'deleteme': "DELETE FROM $table:esc WHERE $fields_values:and", # exact item match
		}
		self.dbapi = None

		if self.settings.get('drivername') == 'sqlite':
			self.dbapi = __import__('sqlite3', fromlist=[''])
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
			# default statements for sqlite
			self.queries.update({
				'insert': "INSERT INTO $table:esc SET $fields_values",
				'upsert': "INSERT OR REPLACE INTO $table:esc ($fields) VALUES ($values)",
				'update': "UPDATE $table:esc SET $fields_values WHERE $indices:and",
			})
		elif self.settings.get('drivername') == 'pgsql':
			self.dbapi = __import__('psycopg2', fromlist=[''])
			#from psycopg2.extras import DictCursor
			self.__dbpool = ConnectionPool('psycopg2', database=self.settings.get('database'),
				user = self.settings.get('username'),
				password = self.settings.get('password', None),
				host = self.settings.get('host', None), # default to unix socket
				port = self.settings.get('port', '5432'),
			#	cursor_factory = DictCursor,
			)
			self.paramstyle = '%s'
			# default statements for postgres
			self.queries.update({
				'insert': "INSERT INTO $table:esc ($fields) VALUES ($values)",
				'update': "UPDATE $table:esc SET $fields_values WHERE $indices:and",
			})

		elif self.settings.get('drivername') == 'mysql':
			self.dbapi = __import__('MySQLdb', fromlist=[''])
			from MySQLdb import cursors
			self.__dbpool = ReconnectingConnectionPool('MySQLdb', db=self.settings.get('database'),
				user = self.settings.get('username'),
				passwd = self.settings.get('password', None),
				host = self.settings.get('host', 'localhost'), # should default to unix socket
				port = self.settings.get('port', 3306),
				cursorclass = cursors.DictCursor,
				charset = 'utf8',
				use_unicode = True,
				# connpool settings
				cp_reconnect = True,
				#cp_noisy = True,
				#cp_min = 1,
				#cp_max = 1,
			)
			self.paramstyle = '%s'
			self.identifier = '`' # MySQL quoting
			# default statements for mysql
			self.queries.update({
				'insert': "INSERT INTO $table:esc ($fields) VALUES ($values)",
			#	'upsert': "REPLACE INTO $table ($fields) VALUES ($values)",
				'upsert': "INSERT INTO $table:esc SET $fields_values ON DUPLICATE KEY UPDATE $fields_values",
				'update': "UPDATE $table:esc SET $fields_values WHERE $indices:and",
			})

		self.queries.update(kwargs.get('queries', {}))

	@classmethod
	def from_crawler(cls, crawler):
		if not crawler.settings.get('SQLMAGIC_DATABASE'):
			raise NotConfigured('No database connection settings found.')

		o = cls(
			settings=crawler.settings.get('SQLMAGIC_DATABASE'),
			stats=crawler.stats,
			queries=crawler.settings.get('SQLMAGIC_QUERIES', {}),
			debug=crawler.settings.getbool('SQLMAGIC_DEBUG')
		)
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

		# Only handle items inheriting SQLItem
		if not isinstance(item, SQLItem):
			return item

		self.stats.inc_value('sqlmagic/total_items_caught')

		# always return original item
		deferred = self.operation(item, spider)
		deferred.addBoth(lambda _: item)
		return deferred

	def operation(self, item, spider):

		def on_insert(result, query, params):
			self.stats.inc_value('sqlmagic/sqlop_success_insert')
			if self.debug:
				qlog = self._log_preparedsql(query, params)
				log.msg('%s executed: %s' % (self.__class__.__name__, qlog), level=log.DEBUG, spider=spider)
			return result

		def on_update(result, query, params):
			self.stats.inc_value('sqlmagic/sqlop_success_update')
			if self.debug:
				qlog = self._log_preparedsql(query, params)
				log.msg('%s executed: %s' % (self.__class__.__name__, qlog), level=log.DEBUG, spider=spider)
			return result

		def on_integrityerror(error, query, params):
			error.trap(self.dbapi.IntegrityError)
			e = error.getErrorMessage()
			self.stats.inc_value('sqlmagic/error_integrity')
			if self.debug:
				qlog = self._log_preparedsql(query, params)
				log.msg('%s failed executing: %s\nError: %s' % (self.__class__.__name__, qlog, e), level=log.INFO, spider=spider)
		#	error.raiseException() # keep bubbling

		def on_operationalerror(error, query, params):
			error.trap(self.dbapi.OperationalError)
			e = error.getErrorMessage()
			self.stats.inc_value('sqlmagic/error_operational')
			if self.debug:
				qlog = self._log_preparedsql(query, params)
				log.msg('%s failed executing: %s\nError: %s' % (self.__class__.__name__, qlog, e), level=log.WARNING, spider=spider)
		#	error.raiseException() # keep bubbling

		def on_seriouserror(error, query, params):
			error.trap(self.dbapi.ProgrammingError, self.dbapi.InterfaceError)
			e = error.getErrorMessage()
			self.stats.inc_value('sqlmagic/error_connection')
			if self.debug:
				qlog = self._log_preparedsql(query, params)
				log.msg('%s FAILED executing: %s\nError: %s' % (self.__class__.__name__, qlog, e), level=log.WARNING, spider=spider)
			error.raiseException() # keep bubbling
			return error

		def update(error, query, params):
			error.trap(self.dbapi.IntegrityError)
			if error.value[0] != 1062: # Duplicate key
				error.raiseException() # keep bubbling
			#e = error.getErrorMessage()
			#if self.debug:
			#	qlog = self._log_preparedsql(query, params)
			#	log.msg('%s got error %s - trying update' % (self.__class__.__name__, e), level=log.DEBUG, spider=spider)
			self.stats.inc_value('sqlmagic/sqlop_update_after_insert_tries')
			d = self.__dbpool.runInteraction(self.transaction, query, params, item, spider)
			d.addCallback(on_update, query, params)
			return d

		# try insert
		query, params = _sql_format(self.queries['insert'], item, paramstyle=self.paramstyle, identifier=self.identifier)
		#query, params = _sql_format(self.queries['upsert'], item, paramstyle=self.paramstyle, identifier=self.identifier)
		deferred = self.__dbpool.runInteraction(self.transaction, query, params, item, spider)
		deferred.addCallback(on_insert, query, params)
		deferred.addErrback(on_seriouserror, query, params)
		deferred.addErrback(on_operationalerror, query, params)
		#deferred.addErrback(on_integrityerror, query, params) # ignore failing inserts before update
		# on failure, update
		query, params = _sql_format(self.queries['update'], item, paramstyle=self.paramstyle, identifier=self.identifier)
		deferred.addErrback(update, query, params)
		deferred.addErrback(on_seriouserror, query, params)
		deferred.addErrback(on_operationalerror, query, params)
		deferred.addErrback(on_integrityerror, query, params)
		deferred.addErrback(self._database_error, item, spider)

	#	deferred = self.insert_or_update((query,params), (update, uparams), item, spider)

		self.stats.inc_value('sqlmagic/total_items_returned')
		return deferred

	def transaction(self, txn, query, params, item, spider):
		self.stats.inc_value('sqlmagic/sqlop_transact_%s' % query[:6].lower())
		txn.execute(query, params)

	"""
	def xtransaction(self, txn, query, params, item, spider):
		# primary key check
		query, params = _sql_format(self.queries['select'], item, paramstyle=self.paramstyle, identifier=self.identifier)
		txn.execute(query, params)
		result = txn.fetchone()
		if result:
			log.msg("Item already in db: (id) %s item:\n%r" % (result['id'], item), level=log.WARNING)

		query, params = _sql_format(self.queries['insert'], item, paramstyle=self.paramstyle, identifier=self.identifier)
		# transaction in thread
		qlog = self._log_preparedsql(query, params)
		try:
			txn.execute(query, params)
		except self.dbapi.IntegrityError as e:
			#spider.log('%s FAILED executing: %s' % (self.__class__.__name__, qlog), level=log.DEBUG)
			query, params = _sql_format(self.queries['update'], item, paramstyle=self.paramstyle, identifier=self.identifier)
			qlog = self._log_preparedsql(query, params)
			try:
				#spider.log('%s executing: %s' % (self.__class__.__name__, qlog), level=log.DEBUG)
				txn.execute(query, params)
			except self.dbapi.OperationalError as e:
				# retrying in new transaction
			#	spider.log('%s errored. Retrying.\nError: %s\nQuery: %s' % (self.__class__.__name__, e, qlog), level=log.WARNING)
			#	self._spool.append((query, params, item))
			#except Exception as e:
				if self.debug:
					spider.log('%s FAILED executing: %s\nError: %s' % (self.__class__.__name__, qlog, e), level=log.WARNING)
				raise
			finally:
				if self.debug:
					spider.log('%s executed: %s' % (self.__class__.__name__, qlog), level=log.DEBUG)
		except self.dbapi.OperationalError as e:
			# also try again
			if self.debug:
				spider.log('%s failed: %s' % (self.__class__.__name__, qlog), level=log.DEBUG)
			raise
		finally:
			if self.debug:
				spider.log('%s executed: %s' % (self.__class__.__name__, qlog), level=log.DEBUG)
	"""

	def _log_preparedsql(self, query, params):
		"""Simulate escaped query for log"""
		for p in params:
			query = re.sub('(\\'+self.paramstyle+r'\d?)', '"%s"' % p, query, count=1)
		return query

	def _database_error(self, e, item, spider=None):
		"""Log exceptions."""
		if spider:
			log.err(e, spider=spider)
		else:
			log.err(e)

	def query(self, sql):
		# run a query in the connection pool
		# parameters for prepared statements must be passed as 'sql=(query, params)'
		# (possible use-case from inside spider code)
		'''Spider Example: build start requests from database results

		from scrapy.exceptions import CloseSpider, NotConfigured
		from ..pipelines.sqlmagic import SQLMagicPipeline

		class MySpider(Spider):
			def spider_opened(self, spider):
				try:
					self.db = SQLMagicPipeline(self.settings.get('SQLMAGIC_DATABASE'))
				except NotConfigured:
					raise CloseSpider('Could not get database settings.')

			@defer.inlineCallbacks
			def db_queries(self, response):
				query = """CALL procedure ()"""
				result = yield self.db.query(query)

				# build requests
				requests = []
				for value in result:
					r = yield self.build_request_fromdb(response, value)
					requests.append(r)

				# queue them
				defer.returnValue(requests)

			def start_requests(self):
				yield Request(self.start_urls[0], callback=self.database_queries)

			def build_request_fromdb(self, response, db):
				# custom logic to convert db result into a request
				r = Request(response.url)
				r.callback = self.parse
				return r
		'''
		if query[:6].lower() in ('select',):
			deferred = self.__dbpool.runQuery(sql)
		if query[:4].lower() in ('call',):
			# potential fail: procedure must run a SELECT for this,
			# otherwise it should do runOperation
			deferred = self.__dbpool.runQuery(sql)
		else:
			deferred = self.__dbpool.runOperation(sql)
		return deferred
