#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Self-contained script to crawl a site

This scripts shows how to crawl a site without settings up a complete project.

# author: darkrho
"""
# for scrapy 0.24

from scrapy.contrib.loader import ItemLoader
from scrapy.item import Item, Field
from scrapy.selector import Selector
from scrapy.spider import Spider


class QuestionItem(Item):
    """Our SO Question Item"""
    title = Field()
    summary = Field()
    tags = Field()

    user = Field()
    posted = Field()

    votes = Field()
    answers = Field()
    views = Field()


class MySpider(Spider):
    """Our ad-hoc spider"""
    name = "myspider"
    start_urls = ["http://stackoverflow.com/"]

    question_list_xpath = '//div[@id="content"]//div[contains(@class, "question-summary")]'

    def parse(self, response):
        sel = Selector(response)

        for qxs in sel.xpath(self.question_list_xpath):
            loader = ItemLoader(QuestionItem(), selector=qxs)
            loader.add_xpath('title', './/h3/a/text()')
            loader.add_xpath('summary', './/h3/a/@title')
            loader.add_xpath('tags', './/a[@rel="tag"]/text()')
            loader.add_xpath('user', './/div[@class="started"]/a[2]/text()')
            loader.add_xpath('posted', './/div[@class="started"]/a[1]/span/@title')
            loader.add_xpath('votes', './/div[@class="votes"]/div[1]/text()')
            loader.add_xpath('answers', './/div[contains(@class, "answered")]/div[1]/text()')
            loader.add_xpath('views', './/div[@class="views"]/div[1]/text()')

            yield loader.load_item()


def main():
    """Setups item signal and run the spider"""
    from twisted.internet import reactor
    from scrapy import signals
    from scrapy.settings import Settings
    from scrapy.crawler import Crawler

    def catch_item(sender, item, **kwargs):
        print "Got:", item

    settings = Settings()

    # set up crawler
    crawler = Crawler(settings)
    # shut off log
    crawler.settings.set('LOG_ENABLED', False, priority='cmdline')
    # set up signal to catch items scraped
    crawler.signals.connect(catch_item,   signal=signals.item_passed)
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)

    crawler.install()
    crawler.configure()

    # schedule spider
    spider = MySpider()
    crawler.crawl(spider)

    # start engine scrapy/twisted
    print "STARTING ENGINE"
    crawler.start()
    reactor.run()
    print "ENGINE STOPPED"


if __name__ == '__main__':
    main()
