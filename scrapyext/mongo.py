"""
MongoDBStorage - pipeline to store scraped data in MongoDB

ITEM_PIPELINES = [
	'project.pipelines.mongo.MongoDBStorage',
]

MONGODB_URI = 'mongodb://localhost:27017'
MONGODB_DATABASE = 'database'
MONGODB_COLLECTION = 'collection'
#MONGODB_UNIQUE_KEY = ['url', 'origin']

"""

from pymongo import errors
from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.read_preferences import ReadPreference
from pymongo import ASCENDING, DESCENDING

from gridfs import GridFS
from gridfs.errors import FileExists, GridFSError

from scrapy import log
from scrapy.exceptions import NotConfigured, DropItem

import copy


class MongoDBStorage(object):
	""" MongoDB item pipeline """

	def __init__(self, settings, stats, **kwargs):
		self.stats = stats
		if not settings.get('MONGODB_DATABASE')
			raise NotConfigured
		if not settings.get('MONGODB_COLLECTION'):
			raise NotConfigured

		self._uri = settings.get('MONGODB_URI', 'mongodb://localhost:27017')
		self._database = settings.get('MONGODB_DATABASE')
		self._collection = settings.get('MONGODB_COLLECTION')

		self._fsync = settings.getbool('MONGODB_FSYNC', False)
		self._replica_set = settings.get('MONGODB_REPLICA_SET', None)
		self._write_concern = settings.getint('MONGODB_WRITE_CONCERN', 0)

		self._unique_key = settings.get('MONGODB_UNIQUE_KEY', None)
		self._ignore_null = settings.getbool('MONGODB_IGNORE_NULL', False)
		self._upsert = settings.getbool('MONGODB_UPSERT', True)

	@classmethod
	def from_crawler(cls, crawler):
		return cls(crawler.settings, crawler.stats)

	def open_spider(self, spider):
		self._build_unique_key()

		if self._replica_set is not None:
			self.connection = MongoReplicaSetClient(
				self._uri,
				replicaSet=self._replica_set,
				w=self._write_concern,
				fsync=self._fsync,
				read_preference=ReadPreference.PRIMARY_PREFERRED)
		else:
			self.connection = MongoClient(
				self._uri,
				fsync=self._fsync,
				read_preference=ReadPreference.PRIMARY)

		self.database = self.connection[self._database]
		self.collection = self.database[self._collection]

		log.msg('Connected to MongoDB "%s", using "%s/%s"' %
			self._uri, self._database, self._collection)

		# ensure index
		if self._unique_key:
			log.msg('Creating index for key %s' % self._unique_key)
			self.collection.ensure_index(self._unique_key.items(), unique=True, sparse=True)

	def close_spider(self, spider):
		del self.collection
		self.connection.disconnect()
		del self.database

	def process_item(self, item, spider):
		self.stats.inc_value('mongostorage/total_item_count') # (duplicate counts with multiple mongo pipelines)

		mongoitem = copy.deepcopy(item) # don't modify original

		idx = None
		if self._unique_key:
			idx = dict([(k, mongoitem[k]) for k, _ in self._unique_key.items()])

		mongoitem = self.verify_item(mongoitem, self.collection, idx, spider)
		if not mongoitem:
			return item

		try:
			dictitem = dict(mongoitem)
		except ValueError:
			return item

		self.stats.inc_value('mongostorage/item_count')

		if self._ignore_null:
			# do not insert None values
			for idx, v in dictitem.items():
				if v is None:
					del dictitem[idx]

		if self._unique_key is None: #and dictitem['unique_key'] is None:
			try:
				self.insert_item(dictitem, self.collection, spider)
				log.msg('Item inserted in MongoDB database %s/%s' % (self._database, self._collection),
					level=log.DEBUG, spider=spider)
				self.stats.inc_value('mongostorage/insert_count')
			except errors.DuplicateKeyError:
				self.stats.inc_value('mongostorage/duplicate_count')
		else:
			# TODO: check return value for success?
			try:
				self.update_item(idx, dictitem, self.collection, spider)
				log.msg('Item updated in MongoDB database %s/%s' % (self._database, self._collection),
					level=log.DEBUG, spider=spider)
				self.stats.inc_value('mongostorage/update_count')
			except errors.DuplicateKeyError:
				self.stats.inc_value('mongostorage/duplicate_count')

		self.stats.inc_value('mongostorage/success_count')

		del dictitem
		del mongoitem
		return item

	def verify_item(self, item, db, idx, spider):
		""" Verify and pre-process item.

		Override in subclass.
		"""
		return item

		''' Example:
		if db.find_one(idx):
			log.msg('Item already exists in MongoDB for key %s' % (idx),
				level=log.DEBUG, spider=spider)
			self.stats.inc_value('mongostorage/duplicate_count')
		else:
			return item
		'''

	def insert_item(self, item, db, spider):
		""" Insert database item.

		Override in subclass.
		"""
		db.insert(item, continue_on_error=True)

	def update_item(self, key, item, db, spider):
		""" Update/upsert database item.

		Override in subclass.
		"""
		db.update(key, item, upsert=self._upsert)

	def _build_unique_key(self):
		if self._unique_key:
			if isinstance(key, basestring):
				self._unique_key = {self._unique_key: ASCENDING}
			if isinstance(self._unique_key, list):
				try:
					self._unique_key = dict(self._unique_key)
				except ValueError:
					self._unique_key = dict([(x, ASCENDING) if isinstance(x, basestring) else x for x in self._unique_key])
			if not isinstance(self._unique_key, dict):
				raise AttributeError

			for key, value in self._unique_key.items():
				if isinstance(value, bool):
					if value:
						value = ASCENDING
					else:
						value = DESCENDING
				if isinstance(value, basestring):
					if value.lower()[:1] == 'a':
						value = ASCENDING
					else:
						value = DESCENDING
				# if int, assume it's ASC(1) or DESC(-1) already
				self._unique_key.update({key: value})


# Example subclassed pipeline
from project.items import Product

class ProductStorage(MongoDBStorage):

	def __init__(self, settings, stats, **kwargs):
		super(ProductStorage, self).__init__(settings, stats, **kwargs)

		self._database = settings.get('MONGODB_PRODUCT_DATABASE')
		self._collection = settings.get('MONGODB_PRODUCT_COLLECTION')
		self._unique_key = settings.get('MONGODB_PRODUCT_UNIQUE_KEY', None)

	def verify_item(self, item, db, spider):
		""" Verify item and return to continue processing """
		if isinstance(item, Product):
			err_msg = ''
			for field, data in item.items():
				if not data:
					err_msg += 'Missing %s in item\n' % (field)
			if err_msg:
				err_msg += 'from %s' % (item)
				log.msg(err_msg, spider=spider, level=log.DEBUG)
				raise DropItem(err_msg)
				#return

			return item

		log.msg('%s ignoring item of type %s' %
			(self.__class__.__name__, item.__class__.__name__),
			level=log.DEBUG, spider=spider)

	def insert_item(self, item, db, spider):
		""" Insert database item.
		"""
		db.insert(item, continue_on_error=True)
		self.postprocess_item(item, spider)

	def update_item(self, key, item, db, spider):
		""" Update/upsert database item.
		"""
		db.update(key, item, upsert=self._upsert)
		self.postprocess_item(item, spider)

	def postprocess_item(self, item, spider):
		""" Custom log output. """
		msg = ''
		for f, d in item.items():
			if f is 'origin' or f is 'ds':
				continue # ignore in log
			if f is 'description' and d:
				# too much content for log
				d = '<description>'
			msg += '(%s) %s / ' % (f, d)
		log.msg('%s saved %s: %s' %
			(self.__class__.__name__, item.__class__.__name__, msg),
			level=log.INFO, spider=spider)


from project.items import Manufacturer

class ManufacturerStorage(MongoDBStorage):

	def __init__(self, settings, stats, **kwargs):
		super(ManufacturerStorage, self).__init__(settings, stats, **kwargs)

		self._database = settings.get('MONGODB_MANUFAC_DATABASE')
		self._collection = settings.get('MONGODB_MANUFAC_COLLECTION')
		self._unique_key = settings.get('MONGODB_MANUFAC_UNIQUE_KEY', None)

	def verify_item(self, item, db, spider):
		if isinstance(item, Manufacturer):
			err_msg = ''
			for field, data in item.items():
				if not data:
				#if field == 'content' and not data:
					err_msg += 'Missing %s in item\n' % (field)
			if err_msg:
				err_msg += 'from %s' % (item)
				log.msg(err_msg, spider=spider, level=log.DEBUG)
				raise DropItem(err_msg)

			return item


class MongoDBGridStorage(object):

	def __init__(self, settings, stats, **kwargs):
		self.stats = stats
		if not settings.get('MONGODB_DATABASE')
			raise NotConfigured

		self._uri = settings.get('MONGODB_URI', 'mongodb://localhost:27017')
		self._database = settings.get('MONGODB_DATABASE')
		self._collection = settings.get('MONGODB_COLLECTION', 'fs')

		self._fsync = settings.getbool('MONGODB_FSYNC', False)
		self._replica_set = settings.get('MONGODB_REPLICA_SET', None)
		self._write_concern = settings.getint('MONGODB_WRITE_CONCERN', 0)

		self.fs = {}

	@classmethod
	def from_crawler(cls, crawler):
		return cls(crawler.settings, crawler.stats)

	def open_spider(self, spider):
		if self._replica_set is not None:
			self.connection = MongoReplicaSetClient(
				self._uri,
				replicaSet=self._replica_set,
				w=self._write_concern,
				fsync=self._fsync,
				read_preference=ReadPreference.PRIMARY_PREFERRED)
		else:
			self.connection = MongoClient(
				self._uri,
				fsync=self._fsync,
				read_preference=ReadPreference.PRIMARY)

		self.database = self.connection[self._database]
		self.fs[spider] = GridFS(self.database, self._collection)

		log.msg('Connected to MongoDB "%s", using GridFS "%s/%s"' %
			self._uri, self._database, self._collection)

	def close_spider(self, spider):
		del self.fs[spider]
		#self.connection.disconnect()
		#del self.database

	def process_item(self, item, spider):
		mongoitem = self.verify_item(item, spider)
		if not mongoitem:
			return item

		try:
			self.fs[spider].put(dict(mongoitem))
			log.msg('Item written to MongoDB GridFS %s/%s' %
				(self._database, self._collection),
				level=log.DEBUG, spider=spider)
			self.stats.inc_value('mongostorage/gridfs_insert_count')
		except FileExists:
			self.stats.inc_value('mongostorage/gridfs_duplicate_count')
		except GridFSError:
			self.stats.inc_value('mongostorage/gridfs_error_count')
			raise

		return item

	def verify_item(self, item, spider):
		""" Verify item before processing.

		Override in subclass.
		"""
		err_msg = ''
		for field, data in item.items():
			if not data:
				err_msg += 'Missing %s in item\n' % (field)
		if err_msg:
			err_msg += 'from %s' % (item)
			raise DropItem(err_msg)

		return item
