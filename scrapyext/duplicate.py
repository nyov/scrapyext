"""
DuplicatesPipeline

Uses a configurable unique key/item field to filter duplicates on.


HashedDuplicatesPipeline

Automatic (exact) duplicates finder, remembering items based on a hash,
optionally using a hashset for every seen itemtype.
"""

from scrapy.exceptions import DropItem
try:
	import cPickle as pickle
except ImportError:
	import pickle


class DuplicatesPipeline(object):

	def __init__(self, settings):
		self.ids_seen = set()
		self.key = settings.get('DUPLICATE_KEY') or raise NotConfigured()

	def process_item(self, item, spider):
		if item[self.key] in self.ids_seen:
			raise DropItem("Duplicate item found: %s" % item)
		else:
			self.ids_seen.add(item[self.key])
			return item


class HashedDuplicatesPipeline(object):

	def __init__(self, settings):
		self.hashsets = settings.getbool('HASHDUPE_SETS') or False
		if self.hashsets:
			self.ids_seen = {}
		else:
			self.ids_seen = set()

	def process_item(self, item, spider):
		type = item.__class__.__name__.lower()
		if self.hashsets:
			hs = self.ids_seen[type] = self.ids_seen.get(type, set())
		else:
			hs = self.ids_seen

		# hashes may differ between interpreter startups;
		# for re-useable hashes, use hashlib algos instead.
		ihash = hash( pickle.dumps(item) )
		if ihash in hs:
			raise DropItem("Duplicate %s found:\n%r" % (item.__class__.__name__, item))
		else:
			self.hs.add(ihash)
			return item
