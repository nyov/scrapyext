"""
SpidernamePipeline

Store spider name in item.
"""

class SpiderFieldPipeline(object):

	def process_item(self, item, spider):
		item['spider'] = spider.name
		return item


class SpidernamePipeline(object):

	def process_item(self, item, spider):
		item['origin'] = spider.ident or spider.name
		return item
