from scrapy.contrib.exporter import XmlItemExporter

# ported XmlItemExporter fix for nested items from git
# issue: https://github.com/scrapy/scrapy/issues/66
class XmlNestedItemExporter(XmlItemExporter):

	def _export_xml_field(self, name, serialized_value):
		self.xg.startElement(name, {})
		if hasattr(serialized_value, 'items'):
			for subname, value in serialized_value.items():
				self._export_xml_field(subname, value)
		elif hasattr(serialized_value, '__iter__'):
			for value in serialized_value:
				self._export_xml_field('value', value)
		else:
			self.xg.characters(serialized_value)
		self.xg.endElement(name)


class XmlPipeline(object):

	def __init__(self):
		self.files = {}

	def open_spider(self, spider):
		file = open('%s.xml' % spider.name, 'w+b')
		self.files[spider] = file
		#self.exporter = XmlItemExporter(file, item_element='event', root_element='events')
		self.exporter = XmlNestedItemExporter(file, item_element='event', root_element='events')
		self.exporter.start_exporting()

	def close_spider(self, spider):
		self.exporter.finish_exporting()
		file = self.files.pop(spider)
		file.close()

	def process_item(self, item, spider):
		self.exporter.export_item(item)
		return item
