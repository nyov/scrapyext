"""
ActiveHttpCacheMiddleware + ExtendedPolicy


DOWNLOADER_MIDDLEWARES = {
	'scrapy.contrib.downloadermiddleware.httpcache.HttpCacheMiddleware': None,
	'project.middlewares.httpcache.ActiveHttpCacheMiddleware': 900,
}
HTTPCACHE_ENABLED = True
HTTPCACHE_POLICY = 'project.middlewares.httpcache.ExtendedPolicy'

"""

from scrapy import signals
from scrapy.contrib.downloadermiddleware.httpcache import HttpCacheMiddleware

class ActiveHttpCacheMiddleware(HttpCacheMiddleware):
	"""Bind spider for use from CachePolicy,
	to inject Requests for cache freshness validation.
	"""

	@classmethod
	def from_crawler(cls, crawler):
		o = cls(crawler.settings, crawler.stats)
		o.crawler = crawler # ADDED
		crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
		crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
		return o

	def spider_opened(self, spider):
		super(ActiveHttpCacheMiddleware, self).spider_opened(spider)
		# pass reference to loaded policy object
		self.policy.crawler = self.crawler # ADDED
		self.policy.spider = spider # ADDED


from time import time
from weakref import WeakKeyDictionary

from scrapy.contrib.httpcache import rfc1123_to_epoch, parse_cachecontrol, RFC2616Policy

from scrapy.http import Request
from scrapy import log

class ExtendedPolicy(RFC2616Policy):
	"""Cache on Content-Length"""

	REQUEST_PRIORITY = 100

	def __init__(self, settings):
		self.ignore_schemes = settings.getlist('HTTPCACHE_IGNORE_SCHEMES')
		self.ignore_http_codes = [int(x) for x in settings.getlist('HTTPCACHE_IGNORE_HTTP_CODES')]
		self._cc_parsed = WeakKeyDictionary()

	def should_cache_response(self, response, request):
		cc = self._parse_cachecontrol(response)
		if response.status in self.ignore_http_codes:
			return False
		# obey directive "Cache-Control: no-store"
		elif 'no-store' in cc:
			return False
		# Never cache 304 (Not Modified) responses
		elif response.status == 304:
			return False
		# Ignore HEAD responses
		elif not response.body and request.method == 'HEAD' and response.status not in (300, 301, 308):
			return False
		# Any hint on response expiration is good
		elif 'max-age' in cc or 'Expires' in response.headers:
			return True
	#	elif 'max-age' in cc:
	#		return True
	#	elif 'Expires' in response.headers:
	#		# ...unless forcibly set into the past by the server
	#		# (lets not pollute cache with unusable responses)
	#		now = time()
	#		date = rfc1123_to_epoch(response.headers.get('Date')) or now
	#		expires = rfc1123_to_epoch(response.headers.get('Expires'))
	#		# When parsing Expires header fails, RFC 2616 section 14.21 says we
	#		# should treat this as an expiration time in the past.
	#		return max(0, expires - date) if expires else False
		# Firefox fallbacks this statuses to one year expiration if none is set
		elif response.status in (300, 301, 308):
			return True
		# Content-Length can be verified through HEAD requests
		elif 'Content-Length' in response.headers: # and not 'Set-Cookie' in response.headers:
			return True
		# Other statuses without expiration requires at least one validator
		elif response.status in (200, 203, 401):
			return 'Last-Modified' in response.headers or 'ETag' in response.headers
		# Any other is probably not eligible for caching
		# Makes no sense to cache responses that does not contain expiration
		# info and can not be revalidated
		else:
			return False

	def is_cached_response_fresh(self, cachedresponse, request):
		cc = self._parse_cachecontrol(cachedresponse)
		ccreq = self._parse_cachecontrol(request)
		if 'no-cache' in cc or 'no-cache' in ccreq:
			return False

		now = time()
		freshnesslifetime = self._compute_freshness_lifetime(cachedresponse, request, now)
		currentage = self._compute_current_age(cachedresponse, request, now)
		if currentage < freshnesslifetime:
			return True
		# Cached response is stale, try to set validators if any
		have_conditional = self._set_conditional_validators(request, cachedresponse)
		if not have_conditional:
			if 'Content-Length' in cachedresponse.headers:
				# HEAD request to calculate Content-Length update
				# this request musn't be caught by the pipeline, else recursion!
				headreq = Request(request.url, method='HEAD',
					headers={'Cache-Control': 'no-store'},
					priority=self.REQUEST_PRIORITY) #dont_filter=True)
				dfd = self.crawler.engine.download(headreq, self.spider)
				dfd.addCallback(self._verify_contentlength, cachedresponse)
				dfd.addErrback(lambda _: False)
				return dfd
		return False

	def _verify_contentlength(self, response, cachedresponse):
		if 'Content-Length' not in response.headers:
			return False
		if response.headers['Content-Length'] == cachedresponse.headers['Content-Length']:
			return True
		return False

	def _set_conditional_validators(self, request, cachedresponse):
		if 'Last-Modified' in cachedresponse.headers:
			request.headers['If-Modified-Since'] = cachedresponse.headers['Last-Modified']
			return True

		if 'ETag' in cachedresponse.headers:
			request.headers['If-None-Match'] = cachedresponse.headers['ETag']
			return True

		return False
