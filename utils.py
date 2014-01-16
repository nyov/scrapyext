# utility functions

def cc_stripped(x, extended=False):
	""" strip control characters from string """
	if extended:
		# also strip extended characters
		return "".join([i for i in x if ord(i) in range(32, 126)])
	return "".join([i for i in x if ord(i) in range(32, 127)])

def zip_list(list, chunks):
	""" zip a flat list into a list of chunk-member tuples """
	return zip(*[iter(list)]*int(chunks))


from scrapy.exceptions import CloseSpider
from scrapy.shell import inspect_response

def inspect(response):
	inspect_response(response)
	raise CloseSpider('Done')
