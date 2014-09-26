from scrapy.item import Field, Item


class DefaultsItem(Item):
	"""
	DefaultsItem - Item with default values.

	Adds default value support for unset Field values.

	Example:
		MyItem(DefaultsItem):
			url  = Field(default='http://example.com/')
			id   = Field(default=0)
			text = Field(default=None)
	"""

	def __getitem__(self, key):
		try:
			return self._values[key]
		except KeyError:
			field = self.fields[key]
			if 'default' in field:
				return field['default']
			raise


class FlexItem(Item):
	"""
	FlexItem - Item with automatic Field declaration.

	Allows arbitrary field names without prior declaration.

	Example:
		MyItem(FlexItem):
			pass
	"""

	def __setitem__(self, key, value):
		if key not in self.fields:
			self.fields[key] = Field()
		self._values[key] = value


# Completely dynamic creation of Item classes
# https://scrapy.readthedocs.org/en/latest/topics/practices.html#dynamic-creation-of-item-classes
from scrapy.item import DictItem, Field

def create_item_class(class_name, field_list):
	field_dict = {}
	for field_name in field_list:
		field_dict[field_name] = Field()

	return type(class_name, (DictItem,), field_dict)
