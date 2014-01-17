from scrapy.contrib.loader import XPathItemLoader as ScrapyItemLoader


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

