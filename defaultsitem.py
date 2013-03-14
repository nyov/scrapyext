# reinstate default value support in newer Scrapy
class DefaultsItem(Item):
    """Item with default values"""

    def __getitem__(self, key):
        try:
            return self._values[key]
        except KeyError:
            field = self.fields[key]
            if 'default' in field:
                return field['default']
            raise
        return self._values[key]
