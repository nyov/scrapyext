"""
GET parameter sort Spider Middleware

Define (known) query parameters and their order,
unknown params will be added at the end of the querystring.

QUERYSORT_ORDER = {
	'shop': 1, 'SessionId': 2, 'c': 3, 'Prod': 4, 'p': 5
}
"""

import urlparse
import urllib

from scrapy import log
from scrapy.http import Request
from scrapy.exceptions import NotConfigured

def querysort(request, sortmap={}):
	if not isinstance(request, Request):
		return

	scheme, netloc, path, params, query, fragment = urlparse.urlparse(request.url)
	q = urlparse.parse_qsl(query, True) # keep empty params

	# sort known fields by ascending order, add unknown fields to end
	q = sorted(q, key=lambda x:sortmap.get(x[0]) or 999)
	query = urllib.urlencode(q)

	url = urlparse.ParseResult(scheme, netloc, path, params, query, fragment).geturl()
	request = request.replace(url=url)

	return request


class QuerySortMiddleware(object):

	def __init__(self, settings):
		self.order = settings.getdict('QUERYSORT_ORDER'):
		if not self.order:
			raise NotConfigured

	@classmethod
	def from_crawler(cls, crawler):
		return cls(crawler.settings)

	def process_spider_output(self, response, result, spider):
		for request in result:
			if isinstance(request, Request):
				request = querysort(request, self.order)
			yield request
