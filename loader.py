try:
	from scrapy.contrib.loader import XPathItemLoader as ScrapyItemLoader # pre-0.22
except ImportError:
	from scrapy.contrib.loader import ItemLoader as ScrapyItemLoader


class ItemLoader(ScrapyItemLoader):
	""" Extended Loader

	for selector resetting.
	"""

	def reset(self, selector=None, response=None):
		if response is not None:
			if selector is None:
				selector = self.default_selector_class(response)
			self.selector = selector
			self.context.update(selector=selector, response=response)
		elif selector is not None:
			self.selector = selector
			self.context.update(selector=selector)

	# keep old behaviour of returning None values from Loader
	# if https://github.com/scrapy/scrapy/pull/556 stays around
	def load_item(self):
		item = self.item
		for field_name in self._values:
			item[field_name] = self.get_output_value(field_name)
		return item
