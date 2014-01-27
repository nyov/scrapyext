"""
MetaCallbackMixin for CrawlSpider

- adding final callbacks to Requests not created from Rule()s

Explanation:

Usually in CrawlSpider, Requests get created from Rules
by matching LinkExtractor objects.

These Request's Responses hit CrawlSpider's internal
parse() method to find further links to follow, before being
sent to their final callback method (from the Rule).

If one needs to create a custom (Form)Request somewhere,
outside of a Rule, it can either have no explicit callback
attached and go through CrawlSpider's parse() for finding
further links to be followed;
_or_ it can have a custom callback to extract response content,
but then bypass CrawlSpider's parse() and not have
other links inside parsed for matching Rules.

Using this mixin, a Response will go through CrawlSpider's parse()
for finding further links from Rules, and finally hit your custom
defined callback (which would usually be attached by the Rule).

This callback is sent with the Request in meta['callback'], and the
immediate Request callback must be the default parse() method of
CrawSpider (simply leave it undefined).

Example:
	class MySpider(MetaCallbackMixin, CrawlSpider):

	Note: observe python multiple inheritance order:
	MySpider > MetaCallbackMixin > CrawlSpider

Usage:
	Request(url,
		meta={
			'callback':self.parse_page, # as function;
			'callback':'parse_page',    # or (class method) string
		},
		# callback=self.parse, # default cb (handle by CrawlSpider)
	)
"""

from scrapy import log


class MetaCallbackMixin(object):
	"""Final response callbacks for custom CrawlSpider requests,
	which were not created from Rule(s).
	"""

	# CrawlSpider override
	def parse_start_url(self, response):
		def get_method(method):
			if callable(method):
				return method
			elif isinstance(method, basestring):
				return getattr(self, method, None)

		method = response.meta.get('callback', None)
		if method:
			callback = get_method(method)
			if not callable(callback):
				raise AttributeError('MetaCallbackMixin: Invalid callback %r, class has no such method' % method)
	#		self.log('MetaCallbackMixin: Callback is self.%s for %s' % (callback.__name__, response.url), log.DEBUG)
			return callback(response)
		else:
			return super(MetaCallbackMixin, self).parse_start_url(response)
