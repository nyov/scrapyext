"""
BlockingCrawlerFromThread

Using Scrapy crawler with a blocking API from a thread

imported from
http://snipplr.com/view/67010/using-scrapy-crawler-with-a-blocking-api-from-a-thread/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: pablo
# date  : Aug 26, 2010
"""
# This script shows how you can use the Scrapy crawler from a thread simulating a blocking API:
#
# The following example shows how you can interact with the crawler in a blocking fashion,
# to run two spiders, one that scrapes 15 items and then another that scrapes 50 items.
# IPython is installed so its console is used, instead of the standard Python console.
#
# For more information see [Twisted Threads](http://twistedmatrix.com/documents/current/core/howto/threading.html)
#
#     $ python this_script.py
#     [ ... Scrapy initialization log here ... ]
#
#     In [1]: items = crawler.crawl('somespider')
#     [ ... somespider log here ... ]
#
#     In [2]: len(items)
#     Out[2]: 15
#
#     In [3]: items2 = crawler.crawl('otherspider')
#     [ ... otherspider log here ... ]
#
#     In [4]: len(items2)
#     Out[4]: 50
#
#     In [5]: ^D
#     [ ... Scrapy termination log here ... ]
#     $

from scrapy import log, signals
from scrapy.utils.console import start_python_console
from scrapy.xlib.pydispatch import dispatcher
from scrapy.conf import settings
from scrapy.crawler import Crawler
from twisted.internet import threads


class BlockingCrawlerFromThread(object):

    def __init__(self, crawler):
        self.crawler = crawler
        dispatcher.connect(self._spider_closed, signals.spider_closed)
        dispatcher.connect(self._item_passed, signals.item_passed)

    def _crawl(self, spider_name):
        spider = self.crawler.spiders.create(spider_name)
        if spider:
            self.items = []
            self.crawler.queue.append_spider(spider)
            self.deferred = defer.Deferred()
            return self.deferred

    def _item_passed(self, item):
        self.items.append(item)

    def _spider_closed(self, spider):
        self.deferred.callback(self.items)

    def crawl(self, spider_name):
        return threads.blockingCallFromThread(reactor, self._crawl, spider_name)

log.start()
#settings.overrides['SPIDER_QUEUE_CLASS'] = 'scrapy.queue.KeepAliveExecutionQueue'
crawler = Crawler(settings)
crawler.install()
crawler.configure()
blocking_crawler = BlockingCrawlerFromThread(crawler)
d = threads.deferToThread(start_python_console, {'crawler': blocking_crawler})
d.addBoth(lambda x: crawler.stop())
crawler.start()
