#!/usr/bin/env python
# -*- coding: utf-8 -*-

# A spider example on using reactor.callLater()
# for delays and repetition.

# scrapy 0.24

import scrapy
from twisted.internet import reactor, defer

from scrapy import Request, log


class Delay(scrapy.Spider):

    name = 'delay'
    allowed_domains = ["localhost"]

    script_url = "http://localhost/"
    start_urls = [script_url]

    def parse(self, response):
        self.log('%s' % response.status, log.INFO)
        self.log('Fetched %s' % response.url, log.INFO)
        return self.later(
            Request(url=self.script_url, dont_filter=True, callback=self.retry),
            timeout=3
        )

    count = 0
    def retry(self, response):
        self.log('%s' % response.status, log.INFO)
        self.log('Retrying %s' % response.url, log.INFO)
        # do this until the day the internet dies...
        self.count += 1
        self.log("Here we go%s..." % (' again'*self.count), log.INFO)
        return self.later(
            Request(url=self.script_url, dont_filter=True, callback=self.retry),
            timeout=1
        )

    def later(self, result, timeout):
        d = defer.Deferred()
        reactor.callLater(timeout, d.callback, result)
        return d


if __name__ == "__main__":
    from scrapy.utils.project import get_project_settings
    from scrapy.crawler import Crawler
    from scrapy import log, signals
    spider = Delay()
    settings = get_project_settings()
    crawler = Crawler(settings)
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    crawler.configure()
    crawler.crawl(spider)
    crawler.start()
    log.start()
    reactor.run()
