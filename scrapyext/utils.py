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


# list(chunks([1, 2, 3, 4, 5, 6, 7, 8, 9], 3))
def chunks(l, n):
	""" Yield successive n-sized chunks from l."""
	for i in xrange(0, len(l), n):
		yield l[i:i+n]


def flattened(l):
	""" recursively flatten a list """
	return reduce(lambda x,y: x+[y] if type(y) != list else x+flattened(y), l,[])


from scrapy.exceptions import CloseSpider
from scrapy.shell import inspect_response

def inspect(response):
	inspect_response(response)
	raise CloseSpider('Done')
