#!/usr/bin/env python
# -*- coding: utf-8 -*-

import scrapy


class MySpider(scrapy.Spider):
	"""Some Spider"""

	name = 'my_spider'

	def __init__(self, **kw):
		super(MySpider, self).__init__(**kw)
		stuff = kw.get('stuff')


if __name__ == "__main__":
	# for scrapy 0.24
	from twisted.internet import reactor
	from scrapy.utils.project import get_project_settings
	from scrapy.crawler import Crawler
	from scrapy import log, signals

	spider = MySpider(stuff='stuff')
	settings = get_project_settings()
	crawler = Crawler(settings)
	crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
	crawler.configure()
	crawler.crawl(spider)
	crawler.start()
	log.start()
	reactor.run() # the script will block here until the spider_closed signal was sent