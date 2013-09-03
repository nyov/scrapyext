import urlparse
import re

from scrapy import log

class SessionFilterMiddleware(object):
	"""Session Filter

	Strip sessions from url 'params', for keeping the cache small.
	"""

	def process_request(self, request, spider):
		#request.headers.setdefault('Accept-Encoding', 'x-gzip,gzip,deflate')
	#	log.msg('Request URL: %s' % request.url, spider=spider, level=log.DEBUG)
		scheme, netloc, path, params, query, fragment = urlparse.urlparse(request.url)
		if 'jsessionid=' in params:
			#params = re.sub(r'jsessionid=.*?;+', '', params) # not sure about multiple params
			params = re.sub(r'jsessionid=.*;*', '', params)
			url = urlparse.ParseResult(scheme, netloc, path, params, query, fragment).geturl()
			request = request.replace(url=url)
	#		log.msg('Request stripped session id', spider=spider, level=log.DEBUG)
			return request

	def process_response(self, request, response, spider):
	#	log.msg('Response URL: %s' % response.url, spider=spider, level=log.DEBUG)
		#content_encoding = response.headers.getlist('Content-Encoding')
		scheme, netloc, path, params, query, fragment = urlparse.urlparse(response.url)
		if 'jsessionid=' in params:
			#params = re.sub(r'jsessionid=.*?;+', '', params) # not sure about multiple params
			params = re.sub(r'jsessionid=.*;*', '', params)
			url = urlparse.ParseResult(scheme, netloc, path, params, query, fragment).geturl()
			response = response.replace(url=url)
	#		log.msg('Stripped session id from response', spider=spider, level=log.DEBUG)
	#	log.msg('Response URL: %s' % response.url, spider=spider, level=log.DEBUG)
		return response
