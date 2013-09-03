from scrapy import log
from scrapy.exceptions import IgnoreRequest

class FilterResponseMiddleware(object):

	def process_response(self, request, response, spider):
		content = 'Please log in'
		if content not in response.body:
			return response

		log.msg(format="Dropped response for %(request)s. No session.", level=log.WARNING, spider=spider, request=request)
		raise IgnoreRequest
