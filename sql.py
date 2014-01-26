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

	itemkeys   = ['{0}{1}{0}'.format(identifier, f) for f in indices] # escaping
	itemfields = ['{0}{1}{0}'.format(identifier, f) for f in item.keys()] # escaping

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


class SQLPipeline(object):

	def __init__(self, settings, **kwargs):
		"""Connect to database in the pool."""

		if not isinstance(settings, dict):
			raise NotConfigured('No database connection settings found.')

		self.settings = settings
		self.debug = kwargs.get('debug', False)
		self.paramstyle = ':'
		self.identifier = '"' # ANSI
		self.queries = {
			'select': "SELECT $fields FROM $table:esc WHERE $indices:and", # select on UniqueFields
			'selectall': "SELECT $fields FROM $table:esc",
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
			# default magics for sqlite
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
			# default magics for postgres
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
			self.identifier = '`' # MySQL
			# default magics for mysql
			self.queries.update({
				'insert': "INSERT INTO $table:esc ($fields) VALUES ($values)",
			#	'upsert': "REPLACE INTO $table ($fields) VALUES ($values)",
				'upsert': "INSERT INTO $table:esc SET $fields_values ON DUPLICATE KEY UPDATE $fields_values",
				'update': "UPDATE $table:esc SET $fields_values WHERE $indices:and",
			})

		self.queries.update(kwargs.get('queries', {}))

	@classmethod
	def from_crawler(cls, crawler):
		if not crawler.settings.get('DATABASE'):
			raise NotConfigured('No database connection settings found.')

		SQL_QUERIES = {
			'deleteme': "DELETE FROM $table:esc WHERE $fields_values:and", # exact item match
			# have indices?
			'fetchone': "SELECT $fields FROM $table:esc WHERE $indices:and LIMIT 1", # if backend supports LIMIT
			'delete'  : "DELETE FROM $table:esc WHERE $indices:and", # match on UniqueFields
		}
		SQL_QUERIES.update(crawler.settings.get('SQL_QUERIES', {}))

		o = cls(
			settings=crawler.settings.get('DATABASE'),
			stats=crawler.stats,
			queries=SQL_QUERIES,
			debug=crawler.settings.getbool('SQLPIPELINE_DEBUG')
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

		# always return original item
		deferred = self.operation(item, spider)
		deferred.addBoth(lambda _: item)
		return deferred

	def operation(self, item, spider):

		def onsuccess(result, query, params):
			if self.debug:
				qlog = self._log_preparedsql(query, params)
				log.msg('%s executed: %s' % (self.__class__.__name__, qlog), level=log.INFO, spider=spider)
			return result

		def onerror(error, query, params):
			error.trap(self.dbapi.IntegrityError) # insert errored
			e = error.getErrorMessage()
			if self.debug:
				qlog = self._log_preparedsql(query, params)
				log.msg('%s failed executing: %s\nError: %s' % (self.__class__.__name__, qlog, e), level=log.WARNING, spider=spider)
			error.raiseException() # keep bubbling
			return error

		def onseriouserror(error, query, params):
			error.trap(self.dbapi.ProgrammingError, self.dbapi.InterfaceErrors)
			e = error.getErrorMessage()
			if self.debug:
				qlog = self._log_preparedsql(query, params)
				log.msg('%s FAILED executing: %s\nError: %s' % (self.__class__.__name__, qlog, e), level=log.ERROR, spider=spider)
			error.raiseException() # keep bubbling
			return error

		def update(error, query, params):
			error.trap(self.dbapi.IntegrityError) # insert errored
			#error.trap(self.dbapi.OperationalError) # db errored - deadlock
			e = error.getErrorMessage()
			if self.debug:
				qlog = self._log_preparedsql(query, params)
				log.msg('%s executing: %s' % (self.__class__.__name__, qlog), level=log.DEBUG, spider=spider)
			deferred = self.__dbpool.runInteraction(self.transaction, query, params, item, spider)
			#deferred = self.__dbpool.runOperation(query, params)
			deferred.addCallback(onsuccess, query, params)
			deferred.addErrback(onerror, query, params)
			return deferred

		# try insert
		query, params = _sql_format(self.queries['insert'], item, paramstyle=self.paramstyle, identifier=self.identifier)
		#query, params = _sql_format(self.queries['upsert'], item, paramstyle=self.paramstyle, identifier=self.identifier)
		deferred = self.__dbpool.runInteraction(self.transaction, query, params, item, spider)
		#deferred = self.__dbpool.runOperation(query, params)
		deferred.addCallback(onsuccess, query, params)
		#deferred.addErrback(onerror, query, params)
		##deferred.addErrback(onseriouserror, query, params)
		# on failure, update
		query, params = _sql_format(self.queries['update'], item, paramstyle=self.paramstyle, identifier=self.identifier)
		deferred.addErrback(update, query, params)
		##deferred.addErrback(onseriouserror, query, params)
	#	deferred.addErrback(self._database_error, item, spider)

	#	deferred = self.insert_or_update((query,params), (update, uparams), item, spider)

		return deferred

	def transaction(self, txn, query, params, item, spider):
		txn.execute(query, params)

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

	def _log_preparedsql(self, query, params):
		""" simulate escaped query for log """
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
		return self.__dbpool.runQuery(sql)
