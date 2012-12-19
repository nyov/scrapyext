"""
MongoDBStorage

ITEM_PIPELINES = [
	'project.pipelines.mongo.MongoDBStorage',
]

MONGODB_HOST = "localhost"
MONGODB_PORT = 27017
MONGODB_DB = "database"
MONGODB_COLLECTION = "collection"

"""
import pymongo

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
		connection = pymongo.Connection(self.server, self.port)
		db = connection[self.db]
		self.collection = db[self.col]
		if self._get_uniq_key() is not None:
			self.collection.create_index(self._get_uniq_key(), unique=True)

	def process_item(self, item, spider):
		err_msg = ''
		for field, data in item.items():
			if not data:
				err_msg += 'Missing %s of item from %s\n' % (field, item['url'])
		if err_msg:
			log.msg(err_msg)
			#raise DropItem(err_msg)
			return item

		if self._get_uniq_key() is None:
			self.collection.insert(dict(item))
		else:
			self.collection.update(
				{self._get_uniq_key(): item[self._get_uniq_key()]},
				dict(item),
				upsert=True)

		log.msg('Item written to MongoDB database %s/%s' % (self.db, self.col),
			level=log.DEBUG, spider=spider)

		return item

	def _get_uniq_key(self):
		if not self.key or self.key == "":
			return None
		return self.key
