"""
MongoDBStorage

ITEM_PIPELINES = [
	'project.pipelines.mongo.MongoDBStorage',
]

MONGODB_URI = 'mongodb://localhost:27017'
MONGODB_DATABASE = 'database'
MONGODB_COLLECTION = 'collection'
#MONGODB_UNIQUE_KEY = 'url'

"""

from pymongo import errors
from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.read_preferences import ReadPreference

from gridfs import GridFS
from gridfs.errors import FileExists, GridFSError

from scrapy import log
from scrapy.exceptions import DropItem


class MongoDBStorage(object):

	def __init__(self, settings, stats, **kwargs):
		self.uri = settings['MONGODB_URI'] or 'mongodb://localhost:27017'
		self.db  = settings['MONGODB_DATABASE']
		self.col = settings['MONGODB_COLLECTION']

		self.fsc = settings['MONGODB_FSYNC'] or False
		self.rep = settings['MONGODB_REPLICA_SET'] or None
		self.wcc = settings['MONGODB_WRITE_CONCERN'] or 0

		self.key = settings['MONGODB_UNIQUE_KEY']

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

		# create index if not existing
		if self._get_uniq_key() is not None:
			self.collection.create_index(self._get_uniq_key(), unique=True)

	def close_spider(self, spider):
		del self.collection
		self.connection.disconnect()
		del self.database

	def process_item(self, item, spider):
		self.stats.inc_value('mongostorage/called_count')

		dictitem = item.copy() # don't modify original
		if not isinstance(item, list):
			dictitem = dict(item)

		self.verify_item(item, spider)

		# do not insert None/null values
		for idx, v in dictitem.items():
			if v is None:
				del dictitem[idx]

		if self._get_uniq_key() is None:
			try:
				self.collection.insert(dictitem, continue_on_error=True)
				self.stats.inc_value('mongostorage/insert_count')
				log.msg('Item inserted in MongoDB database %s/%s' % (self.db, self.col),
					level=log.DEBUG, spider=spider)
			except errors.DuplicateKeyError:
				self.stats.inc_value('mongostorage/dupkey_error_count')
		else:
			if self.collection.find_one({ self._get_uniq_key(): dictitem[self._get_uniq_key()] }):
				log.msg('Item already exists in MongoDB %s' % (dictitem[self._get_uniq_key()]), level=log.DEBUG, spider=spider)
				return item

			self.collection.update(
				{self._get_uniq_key(): item[self._get_uniq_key()]},
				dictitem,
				upsert=True)
			self.stats.inc_value('mongostorage/upsert_count')
			log.msg('Item upserted in MongoDB database %s/%s' % (self.db, self.col),
				level=log.DEBUG, spider=spider)

	#	log.msg('Item written to MongoDB database %s/%s' % (self.db, self.col),
	#		level=log.DEBUG, spider=spider)

		del dictitem
		return item

	def verify_item(self, item, spider):
		""" Verify item before processing.
		"""
		err_msg = ''
		for field, data in item.items():
			if not data:
				err_msg += 'Missing %s in item\n' % (field)
		if err_msg:
			err_msg += 'from %s' % (item)
			log.msg(err_msg, spider=spider, level=log.INFO)
			raise DropItem(err_msg)

	def insert_item(self, item, spider):
		""" Insert database item.
		"""

	def update_item(self, item, spider):
		""" Update/upsert database item.
		"""

	def _get_uniq_key(self):
		if not self.key or self.key == "":
			return None
		return self.key


class ProductStorage(MongoDBStorage):

	def __init__(self, settings, stats, **kwargs):
		super(ProductStorage, self).__init__(settings, stats, **kwargs)

		self.db	 = settings['MONGODB_PRODUCT_DATABASE']
		self.col = settings['MONGODB_PRODUCT_COLLECTION']
		self.key = settings['MONGODB_PRODUCT_UNIQUE_KEY']

class ManufacturerStorage(MongoDBStorage):

	def __init__(self, settings, stats, **kwargs):
		super(ManufacturerStorage, self).__init__(settings, stats, **kwargs)

		self.db	 = settings['MONGODB_MANUFAC_DATABASE']
		self.col = settings['MONGODB_MANUFAC_COLLECTION']
		self.key = settings['MONGODB_MANUFAC_UNIQUE_KEY']


class MongoDBGridStorage(object):

	def __init__(self, settings, stats, **kwargs):
		self.uri = settings['MONGODB_URI'] or 'mongodb://localhost:27017'
		self.db	 = settings['MONGODB_DATABASE']
		self.col = settings['MONGODB_COLLECTION'] or 'fs'

		self.fsc = settings['MONGODB_FSYNC'] or False
		self.rep = settings['MONGODB_REPLICA_SET'] or None
		self.wcc = settings['MONGODB_WRITE_CONCERN'] or 0

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
		err_msg = ''
		for field, data in item.items():
			if not data:
				err_msg += 'Missing %s in item\n' % (field)
		if err_msg:
			err_msg += 'from %s' % (item)
			raise DropItem(err_msg)

		self.fs[spider].put(dict(item))

		log.msg('Item written to MongoDB GridFS %s/%s' % (self.db, self.fs[spider]),
			level=log.DEBUG, spider=spider)

		return item
