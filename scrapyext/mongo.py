"""
MongoDBStorage

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


class MongoDBStorage(object):
	""" MongoDB item pipeline """

	def __init__(self, settings, stats, **kwargs):
		self.stats = stats
		if not settings.get('MONGODB_DATABASE')
			raise NotConfigured
		if not settings.get('MONGODB_COLLECTION')
			raise NotConfigured

		self.uri = settings.get('MONGODB_URI', 'mongodb://localhost:27017')
		self.db  = settings.get('MONGODB_DATABASE')
		self.col = settings.get('MONGODB_COLLECTION')

		self.fsc = settings.getbool('MONGODB_FSYNC', False)
		self.rep = settings.get('MONGODB_REPLICA_SET', None)
		self.wcc = settings.getint('MONGODB_WRITE_CONCERN', 0)

		self.key = settings.get('MONGODB_UNIQUE_KEY', None)
		if not type(self.key) == 'list':
			raise AttributeError

	@classmethod
	def from_crawler(cls, crawler):
		return cls(crawler.settings, crawler.stats)

	def open_spider(self, spider):
		if self.rep is not None:
			self.connection = MongoReplicaSetClient(
				self.uri,
				replicaSet=self.rep,
				w=self.wcc,
				fsync=self.fsc,
				read_preference=ReadPreference.PRIMARY_PREFERRED)
		else:
			self.connection = MongoClient(
				self.uri,
				fsync=self.fsc,
				read_preference=ReadPreference.PRIMARY)

		self.database = self.connection[self.db]
		self.collection = self.database[self.col]

		log.msg('Connected to MongoDB "%s", using "%s/%s"' %
			self.uri, self.db, self.col)

		# ensure index
		# TODO: list of tuples for compound index
		if self.key is not None:
			key = {}
			[key.update({k: item[k]}) for k in self.key]
			self.collection.ensure_index(key, unique=True)

	def close_spider(self, spider):
		del self.collection
		self.connection.disconnect()
		del self.database

	def process_item(self, item, spider):
		self.stats.inc_value('mongostorage/called_count')

		mongoitem = item.copy() # don't modify original
		mongoitem = self.verify_item(mongoitem, spider)

		dictitem = mongoitem
		if not isinstance(mongoitem, list):
			dictitem = dict(mongoitem)

		# do not insert None/null values
		for idx, v in dictitem.items():
			if v is None:
				del dictitem[idx]

		if self.key is None #and dictitem['unique_key'] is None:
			try:
				self.collection.insert(dictitem, continue_on_error=True)
				log.msg('Item inserted in MongoDB database %s/%s' % (self.db, self.col),
					level=log.DEBUG, spider=spider)
				self.stats.inc_value('mongostorage/insert_count')
			except errors.DuplicateKeyError:
				self.stats.inc_value('mongostorage/dupkey_error_count')
		else:
			key = {}
			[key.update({k: dictitem[k]}) for k in self._unique_key]

			if self.collection.find_one(key):
				log.msg('Item already exists in MongoDB for key %s' % key,
					level=log.DEBUG, spider=spider)
				self.stats.inc_value('mongostorage/dupkey_error_count')
				return item

			# TODO: check return value for success?
			self.collection.update(key, dictitem, upsert=True)
			log.msg('Item upserted in MongoDB database %s/%s' % (self.db, self.col),
				level=log.DEBUG, spider=spider)
			self.stats.inc_value('mongostorage/upsert_count')

		self.stats.inc_value('mongostorage/success_count')

		del dictitem
		return item

	def verify_item(self, item, spider):
		""" Verify item before processing.

		Override in subclass.
		"""
		return item

	def insert_item(self, item, spider):
		""" Insert database item.

		Override in subclass.
		"""

	def update_item(self, item, spider):
		""" Update/upsert database item.

		Override in subclass.
		"""


class ProductStorage(MongoDBStorage):

	def __init__(self, settings, stats, **kwargs):
		super(ProductStorage, self).__init__(settings, stats, **kwargs)

		self.db	 = settings['MONGODB_PRODUCT_DATABASE']
		self.col = settings['MONGODB_PRODUCT_COLLECTION']
		self.key = settings['MONGODB_PRODUCT_UNIQUE_KEY']

	def verify_item(self, item, spider):
		err_msg = ''
		for field, data in item.items():
			if field == 'content' and not data:
				err_msg += 'Missing %s in item\n' % (field)
		if err_msg:
			err_msg += 'from %s' % (item)
			log.msg(err_msg, spider=spider, level=log.DEBUG)
			#raise DropItem(err_msg)
			return

		return item


class ManufacturerStorage(MongoDBStorage):

	def __init__(self, settings, stats, **kwargs):
		super(ManufacturerStorage, self).__init__(settings, stats, **kwargs)

		self.db	 = settings['MONGODB_MANUFAC_DATABASE']
		self.col = settings['MONGODB_MANUFAC_COLLECTION']
		self.key = settings['MONGODB_MANUFAC_UNIQUE_KEY']

	def verify_item(self, item, spider):
		err_msg = ''
		for field, data in item.items():
			if not data:
				err_msg += 'Missing %s in item\n' % (field)
		if err_msg:
			err_msg += 'from %s' % (item)
			log.msg(err_msg, spider=spider, level=log.DEBUG)
			#raise DropItem(err_msg)
			return

		return item


class MongoDBGridStorage(object):

	def __init__(self, settings, stats, **kwargs):
		self.stats = stats
		if not settings.get('MONGODB_DATABASE')
			raise NotConfigured

		self.uri = settings.get('MONGODB_URI', 'mongodb://localhost:27017')
		self.db  = settings.get('MONGODB_DATABASE')
		self.col = settings.get('MONGODB_COLLECTION', 'fs')

		self.fsc = settings.getbool('MONGODB_FSYNC', False)
		self.rep = settings.get('MONGODB_REPLICA_SET', None)
		self.wcc = settings.getint('MONGODB_WRITE_CONCERN', 0)

		self.fs = {}

	@classmethod
	def from_crawler(cls, crawler):
		return cls(crawler.settings, crawler.stats)

	def open_spider(self, spider):
		if self.rep is not None:
			self.connection = MongoReplicaSetClient(
				self.uri,
				replicaSet=self.rep,
				w=self.wcc,
				fsync=self.fsc,
				read_preference=ReadPreference.PRIMARY_PREFERRED)
		else:
			self.connection = MongoClient(
				self.uri,
				fsync=self.fsc,
				read_preference=ReadPreference.PRIMARY)

		self.database = self.connection[self.db]
		self.fs[spider] = GridFS(self.database, self.col)

		log.msg('Connected to MongoDB "%s", using GridFS "%s/%s"' %
			self.uri, self.db, self.col)

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
				(self.db, self.fs),
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
