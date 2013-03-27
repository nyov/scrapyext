from scrapy.contrib.exporter import XmlItemExporter

class XmlPipeline(object):
	def __init__(self):
		self.files = {}

	def open_spider(self, spider):
		file = open('%s.xml' % spider.name, 'w+b')
		self.files[spider] = file
		self.exporter = XmlItemExporter(file, item_element='event', root_element='events')
		self.exporter.start_exporting()

	def close_spider(self, spider):
		self.exporter.finish_exporting()
		file = self.files.pop(spider)
		file.close()

	def process_item(self, item, spider):
		self.exporter.export_item(item)
		return item
