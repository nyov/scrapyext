from scrapy.conf import settings
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


# with fixed output ordering
class OrderedXmlNestedItemExporter(XmlItemExporter):

	# fields in the order to be output
	order = {'date':1, 'time':2, 'league':3, 'venue':4, 'name':5, 'score':6}

	def export_item(self, item):
		self.xg.startElement(self.item_element, {})
		for name, value in sorted(self._get_serialized_fields(item, default_value=''), key=lambda x:self.order.get(x[0]) or 99):
			self._export_xml_field(name, value)
		self.xg.endElement(self.item_element)

	def _export_xml_field(self, name, serialized_value):
		self.xg.startElement(name, {})
		if hasattr(serialized_value, 'items'):
			for subname, value in sorted(serialized_value.items(), key=lambda x:self.order.get(x[0]) or 99):
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
		self.path = settings['XML_OUTPUT_PATH'] or ''

	def open_spider(self, spider):
		file = open('%s%s.xml' % (self.path, spider.name), 'w+b')
		self.files[spider] = file
		#self.exporter = XmlItemExporter(file, item_element='event', root_element='events')
		#self.exporter = XmlNestedItemExporter(file, item_element='event', root_element='events')
		self.exporter = OrderedXmlNestedItemExporter(file, item_element='event', root_element='events')
		self.exporter.start_exporting()

	def close_spider(self, spider):
		self.exporter.finish_exporting()
		file = self.files.pop(spider)
		file.close()

	def process_item(self, item, spider):
		self.exporter.export_item(item)
		return item
