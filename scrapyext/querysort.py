"""
GET parameter sort Spider Middleware

"""

import urlparse
import urllib

from scrapy import log
from scrapy.http import Request
from scrapy.exceptions import NotConfigured

class QuerySortMiddleware(object):

	def __init__(self, settings):
		if not settings.getbool('QUERYSORT_ENABLED'):
			raise NotConfigured

	@classmethod
	def from_crawler(cls, crawler):
		return cls(crawler.settings)

	def process_spider_output(self, response, result, spider):
		for x in result:
			if isinstance(x, Request):
				# resort the parameters for broken sites which do redirects
				# or simliar to "fix" query field order (endless redirects)

				# order of known fields
				order = {'shop': 1, 'SessionId': 2, 'c': 3, 'Prod': 4, 'p': 5}

				scheme, netloc, path, params, query, fragment = urlparse.urlparse(x.url)
				q = urlparse.parse_qsl(query, True) # keep empty params

				# sort known fields by ascending order, add unknown fields to end
				q = sorted(q, key=lambda x:order.get(x[0]) or 99)
				query = urllib.urlencode(q)

				url = urlparse.ParseResult(scheme, netloc, path, params, query, fragment).geturl()
				x = x.replace(url=url)

			#	msg = "Request parameters resorted"
			#	log.msg(msg, spider=spider, level=log.DEBUG)

			yield x
