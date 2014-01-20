"""
 Monkey-patched SelectorList

"""

from scrapy.selector.unified import SelectorList as ScrapySelectorList

class SelectorList(ScrapySelectorList):
	def extract(self, pos=None):
		if pos is 'first':
			return ''.join(self[:1].extract()) or None
		if pos is 'last':
			return ''.join(self[-1:].extract()) or None
		return super(SelectorList, self).extract()

	def re(self, regex, pos=None):
		if pos is 'first':
			return ''.join(self[:1].re(regex)) or None
		if pos is 'last':
			return ''.join(self[-1:].re(regex)) or None
		return super(SelectorList, self).re(regex)

import scrapy
scrapy.selector.unified.SelectorList = SelectorList

from scrapy.selector import Selector
