class SpidernamePipeline(object):
	""" Write spider name to item. """

	def process_item(self, item, spider):
		item['origin'] = unicode(spider.ident) or unicode(spider.name)
		return item
