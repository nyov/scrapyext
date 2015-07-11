#!/usr/bin/env python
# -*- coding: utf-8 -*-

import scrapy
from scrapy.settings import Settings


class MySpider(scrapy.Spider):
    """Some Spider"""

    name = 'my_spider'

    def __init__(self, **kw):
        super(MySpider, self).__init__(**kw)
        stuff = kw.get('stuff')


if __name__ == "__main__":
    # for scrapy 0.24
    from twisted.internet import reactor
    from scrapy.crawler import Crawler
    from scrapy import log, signals

    global crawlercount
    crawlercount = 0
    def crawlstack():
        global crawlercount
        crawlercount -= 1
        if crawlercount < 1:
            reactor.stop()

    def setup_crawler(stuff):
        spider = MySpider(stuff=stuff)
        settings = Settings()
        #settings.setdict(env_overrides, priority='project')
        crawler = Crawler(settings)
        crawler.signals.connect(crawlstack, signal=signals.spider_closed)
        crawler.configure()
        crawler.crawl(spider)
        crawler.start()

    for stuff in ['stuff1', 'stuff2']:
        crawlercount += 1
        setup_crawler(stuff)

    log.start()
    reactor.run() # the script will block here until the spider_closed signal was sent
