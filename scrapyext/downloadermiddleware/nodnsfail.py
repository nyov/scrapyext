"""
Handle DNSLookupError, and ignore it.

Required settings:

DNSFAILIGNORE_ENABLED = True

DOWNLOADER_MIDDLEWARES = {
    # run AFTER RetryMiddleware, do the normal retries first
    'scrapy.contrib.downloadermiddleware.nodnsfail.NoDNSFailMiddleware': 499,
    # run BEFORE RetryMiddleware, instantly discard (might discard for timeout reasons also)
    'scrapy.contrib.downloadermiddleware.nodnsfail.NoDNSFailMiddleware': 501,
}
"""

from twisted.internet.error import DNSLookupError

from scrapy import log
from scrapy.exceptions import NotConfigured
from scrapy.http import TextResponse
from scrapy.exceptions import IgnoreRequest


class NoDNSFailMiddleware(object):

    def __init__(self, settings):
        if not settings.getbool('DNSFAILIGNORE_ENABLED'):
            raise NotConfigured

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_exception(self, request, exception, spider):
        if isinstance(exception, DNSLookupError):
            log.msg(format="NoDNSFail caught a lookup error, cheerfully ignoring it :)",
                    level=log.DEBUG, spider=spider) #, reason=exception)
            # Oooh, where did it go
            return TextResponse(url='DNSLookupError')

    def process_response(self, request, response, spider):
        if response.url is 'DNSLookupError':
            # Ah, there it went... bye bye
            raise IgnoreRequest
        return response
