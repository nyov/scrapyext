"""
DefaultsItem

Adds default value support for nonexisting Item Fields.

Example:

	MyItem(Item):
		url  = Field(default='http://example.com/')
		id   = Field(default=0)
		text = Field(default=None)

"""
from scrapy.item import Item


class DefaultsItem(Item):
	""" Item with default values """

	def __getitem__(self, key):
		try:
			return self._values[key]
		except KeyError:
			field = self.fields[key]
			if 'default' in field:
				return field['default']
			raise
