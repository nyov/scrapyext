#!/usr/bin/python
"""
Using Scrapy from a script

This snippet can be used to run scrapy spiders independent of
scrapyd or the scrapy command line tool and use it from a script.

The multiprocessing library is used in order to work around a bug
in Twisted, in which you cannot restart an already running reactor
or in this case a scrapy instance.

[Here](http://groups.google.com/group/scrapy-users/browse_thread/thread/f332fc5b749d401a)
is the mailing-list discussion for this snippet.

imported from
http://snipplr.com/view/67006/using-scrapy-from-a-script/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: joehillen
# date  : Oct 24, 2010
"""

import os
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'project.settings') #Must be at the top before other imports

from scrapy import log, signals, project
from scrapy.xlib.pydispatch import dispatcher
from scrapy.conf import settings
from scrapy.crawler import CrawlerProcess
from multiprocessing import Process, Queue

class CrawlerScript():

    def __init__(self):
        self.crawler = CrawlerProcess(settings)
        if not hasattr(project, 'crawler'):
            self.crawler.install()
        self.crawler.configure()
        self.items = []
        dispatcher.connect(self._item_passed, signals.item_passed)

    def _item_passed(self, item):
        self.items.append(item)

    def _crawl(self, queue, spider_name):
        spider = self.crawler.spiders.create(spider_name)
        if spider:
            self.crawler.queue.append_spider(spider)
        self.crawler.start()
        self.crawler.stop()
        queue.put(self.items)

    def crawl(self, spider):
        queue = Queue()
        p = Process(target=self._crawl, args=(queue, spider,))
        p.start()
        p.join()
        return queue.get(True)

# Usage
if __name__ == "__main__":
    log.start()

    """
    This example runs spider1 and then spider2 three times.
    """
    items = list()
    crawler = CrawlerScript()
    items.append(crawler.crawl('spider1'))
    for i in range(3):
        items.append(crawler.crawl('spider2'))
    print items
