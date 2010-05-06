from scrapy import log, item

class LogVisitedMiddleware(object):
	""" SpiderMiddleware that records urls in a visited log, but only if there
	are items in the results.
	"""
	def __init__(self):
		fname = 'visited.log'
		log.msg('logging visited urls to %s' % fname, level=log.INFO)
		self.visited_log = open(fname, 'a')
	
	def __del__(self):
		self.visited_log.close()
	
	def process_spider_output(self, response, result, spider):
		# make sure we have a list and not an exhaustable iterable
		results = [r for r in result]
		
		if any([isinstance(r, item.Item) for r in results]):
			self.visited_log.write(response.url+'\n')
			self.visited_log.flush()
			log.msg('visited items at %s' % response.url, level=log.DEBUG)
		
		return results
