"""
URL Canonicalizer Spider middleware

imported from
http://snipplr.com/view/67007/url-canonicalizer-spider-middleware/

"""
# A spider middleware to canonicalize the urls of all requests generated from a spider.

from scrapy.http import Request
from scrapy.utils.url import canonicalize_url

class UrlCanonicalizerMiddleware(object):
    def process_spider_output(self, response, result, spider):
        for r in result:
            if isinstance(r, Request):
                curl = canonicalize_url(r.url)
                if curl != r.url:
                    r = r.replace(url=curl)
            yield r

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: pablo
# date  : Sep 07, 2010
