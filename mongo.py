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
from pymongo.read_preferences import ReadPreference

from gridfs import GridFS
from gridfs.errors import FileExists, GridFSError

from scrapy import log
from scrapy.conf import settings
from scrapy.exceptions import DropItem


class MongoDBStorage(object):

	def __init__(self):
		self.uri = settings['MONGODB_URI']
		self.db	 = settings['MONGODB_DATABASE']
		self.col = settings['MONGODB_COLLECTION']
		self.key = settings['MONGODB_UNIQUE_KEY']

	def open_spider(self, spider):
		self.connection = MongoClient(self.uri, fsync=False, read_preference=ReadPreference.PRIMARY)
		self.database = self.connection[self.db]
		self.collection = self.database[self.col]

		if self._get_uniq_key() is not None:
			self.collection.create_index(self._get_uniq_key(), unique=True)

	def close_spider(self, spider):
		del self.collection
		self.connection.disconnect()
		del self.database

	def process_item(self, item, spider):
		err_msg = ''
		for field, data in item.items():
			if not data:
				err_msg += 'Missing %s in item\n' % (field)
		if err_msg:
			err_msg += 'from %s' % (item)
			log.msg(err_msg, level=log.INFO)
			#raise DropItem(err_msg)
			return item

		if self._get_uniq_key() is None:
			self.collection.insert(dict(item))
			log.msg('Item inserted in MongoDB database %s/%s' % (self.db, self.col),
				level=log.DEBUG, spider=spider)
		else:
			if self.collection.find_one({ self._get_uniq_key(): item[self._get_uniq_key()] }):
				log.msg('Item already exists in MongoDB %s' % (item[self._get_uniq_key()]), level=log.DEBUG, spider=spider)
				return item

			self.collection.update(
				{self._get_uniq_key(): item[self._get_uniq_key()]},
				dict(item),
				upsert=True)
			log.msg('Item upserted in MongoDB database %s/%s' % (self.db, self.col),
				level=log.DEBUG, spider=spider)

	#	log.msg('Item written to MongoDB database %s/%s' % (self.db, self.col),
	#		level=log.DEBUG, spider=spider)

		return item

	def _get_uniq_key(self):
		if not self.key or self.key == "":
			return None
		return self.key


class ProductStorage(MongoDBStorage):

	def __init__(self):
		self.db	 = settings['MONGODB_PRODUCT_DATABASE']
		self.col = settings['MONGODB_PRODUCT_COLLECTION']
		self.key = settings['MONGODB_PRODUCT_UNIQUE_KEY']

class ManufacturerStorage(MongoDBStorage):

	def __init__(self):
		self.db	 = settings['MONGODB_MANUFAC_DATABASE']
		self.col = settings['MONGODB_MANUFAC_COLLECTION']
		self.key = settings['MONGODB_MANUFAC_UNIQUE_KEY']


class MongoDBGridStorage(object):

	def __init__(self):
		self.uri = settings['MONGODB_URI']
		self.db	 = settings['MONGODB_DATABASE']
		self.col = settings['MONGODB_COLLECTION'] or 'fs'
		self.fs = {}

	def open_spider(self, spider):
		self.connection = MongoClient(self.uri, fsync=False, read_preference=ReadPreference.PRIMARY)
		self.database = self.connection[self.db]
		self.fs[spider] = GridFS(self.database, self.col)

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
