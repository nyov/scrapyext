from scrapy import log
from scrapy import signals
from scrapy.exceptions import DropItem

class DuplicatesPipeline(object):

	def __init__(self):
		self.ids_seen = set()

	def process_item(self, item, spider):
		if item['id'] in self.ids_seen:
			raise DropItem("Duplicate item found: %s" % item)
		else:
			self.ids_seen.add(item['id'])
			return item

try:
	import cPickle as pickle
except:
	import pickle

class DuplicateHashPipeline(object):

	def __init__(self):
		self.products = set()
		self.manufacturers = set()

	def process_item(self, item, spider):
		itype = item['itemtype']
		if itype == 'product':
			ihash = hash( pickle.dumps(item) )
			if ihash in self.products:
				raise DropItem("Duplicate product found: %s" % item['id'])
			else:
				self.products.add(ihash)
		if itype == 'manufacturer':
			ihash = hash( pickle.dumps(item) )
			if ihash in self.manufacturers:
				raise DropItem("Duplicate manufacturer found: %s" % item['id'])
			else:
				self.manufacturers.add(ihash)
		return item
