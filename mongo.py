"""
MongoDBStorage

ITEM_PIPELINES = [
	'project.pipelines.mongo.MongoDBStorage',
]

MONGODB_HOST = "localhost"
MONGODB_PORT = 27017
MONGODB_DB = "database"
# (not for gridfs)
MONGODB_COLLECTION = "collection"
#MONGODB_UNIQ_KEY = "url"

"""

from pymongo import Connection
from gridfs import GridFS
from gridfs.errors import FileExists, GridFSError

from scrapy import log
from scrapy.conf import settings
from scrapy.exceptions import DropItem


class MongoDBStorage(object):

	def __init__(self):
		self.host = settings['MONGODB_HOST']
		self.port = settings['MONGODB_PORT']
		self.db = settings['MONGODB_DB']
		self.col = settings['MONGODB_COLLECTION']
		self.key = settings['MONGODB_UNIQ_KEY']

	def open_spider(self, spider):
		connection = Connection(self.host, self.port)
		db = connection[self.db]
		self.collection = db[self.col]

		if self._get_uniq_key() is not None:
			self.collection.create_index(self._get_uniq_key(), unique=True)

	def close_spider(self, spider):
		del self.collection

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
		self.col = settings['MONGODB_PRODUCT_COLLECTION']
		self.key = settings['MONGODB_PRODUCT_UNIQ_KEY']

class ManufacturerStorage(MongoDBStorage):

	def __init__(self):
		self.col = settings['MONGODB_MANUFAC_COLLECTION']
		self.key = settings['MONGODB_MANUFAC_UNIQ_KEY']


class MongoDBGridStorage(object):

	def __init__(self):
		self.host = settings['MONGODB_HOST']
		self.port = settings['MONGODB_PORT']
		self.db = settings['MONGODB_DB']
		self.fs = {}
		self.connection = Connection(self.host, self.port)[self.db]

	def open_spider(self, spider):
		self.fs[spider] = GridFS(self.connection, 'storage')

	def close_spider(self, spider):
		del self.fs[spider]

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
