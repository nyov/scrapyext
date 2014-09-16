"""
BlockingCrawlerFromThread

Using Scrapy crawler with a blocking API from a thread

imported from
http://snipplr.com/view/67010/using-scrapy-crawler-with-a-blocking-api-from-a-thread/

# author: pablo
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

from scrapy import signals


class BlockingCrawlerFromThread(object):

    def __init__(self, crawler):
        self.crawler = crawler
        crawler.signals.connect(self._spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(self._item_passed, signal=signals.item_passed)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

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


from twisted.internet import threads
from scrapy import log
from scrapy.utils.console import start_python_console
from scrapy.conf import settings
from scrapy.crawler import CrawlerProcess

log.start()
# FIXME
settings.overrides['QUEUE_CLASS'] = 'scrapy.core.queue.KeepAliveExecutionQueue'
crawler_process = CrawlerProcess(settings)
crawler_process.configure()
crawler_process.install()
blocking_crawler = BlockingCrawlerFromThread(crawler)
d = threads.deferToThread(start_python_console, {'crawler': blocking_crawler})
# FIXME
d.addBoth(lambda x: crawler_process.stop())
crawler_process.start()
