class SpidernamePipeline(object):
	""" Write spider name to item. """

	def process_item(self, item, spider):
		item['origin'] = spider.ident or spider.name
		return item
