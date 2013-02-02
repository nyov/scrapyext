"""
Avoid downloading pages which exceed a certain size

imported from
http://snipplr.com/view/66993/avoid-downloading-pages-which-exceed-a-certain-size/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: pablo
# date  : Sep 16, 2011
"""
# This snippet was taken from the old wiki.
#
# You can do this by overriding the Scrapy HTTP Client Factory, with the following (undocumented) setting:
#
#     DOWNLOADER_HTTPCLIENTFACTORY = 'myproject.downloader.LimitSizeHTTPClientFactory'
#

MAX_RESPONSE_SIZE = 1048576 # 1Mb

from scrapy.core.downloader.webclient import ScrapyHTTPClientFactory, ScrapyHTTPPageGetter

class LimitSizePageGetter(ScrapyHTTPPageGetter):

    def handleHeader(self, key, value):
        ScrapyHTTPPageGetter.handleHeader(self, key, value)
        if key.lower() == 'content-length' and int(value) > MAX_RESPONSE_SIZE:
            self.connectionLost('oversized')

class LimitSizeHTTPClientFactory(ScrapyHTTPClientFactory):

     protocol = LimitSizePageGetter
