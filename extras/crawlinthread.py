#!/usr/bin/env python
"""
Run Scrapy crawler in a thread - works on Scrapy 0.8

When you run the Scrapy crawler from a program, the code blocks
until the Scrapy crawler is finished. This is due to how Twisted
(the underlying asynchronous network library) works.
This prevents using the Scrapy crawler from scripts or other code.

To circumvent this issue you can run the Scrapy crawler in a thread
with this code.

Keep in mind that this code is mainly for illustrative purposes and
far from production ready.

Also the code was only tested with Scrapy 0.8, and will probably
need some adjustments for newer versions (since the core API isn't
stable yet), but you get the idea.

imported from
http://snipplr.com/view/67015/run-scrapy-crawler-in-a-thread/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: pablo
# date  : Aug 11, 2010
"""

# for scrapy 0.8
# hopelessly outdated

import threading, Queue

from twisted.internet import reactor

from scrapy.xlib.pydispatch import dispatcher
from scrapy.core.manager import scrapymanager
from scrapy.core.engine import scrapyengine
from scrapy.core import signals


class CrawlerThread(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.running = False

    def run(self):
        self.running = True
        scrapymanager.configure(control_reactor=False)
        scrapymanager.start()
        reactor.run(installSignalHandlers=False)

    def crawl(self, *args):
        if not self.running:
            raise RuntimeError("CrawlerThread not running")
        self._call_and_block_until_signal(signals.spider_closed, \
            scrapymanager.crawl, *args)

    def stop(self):
        reactor.callFromThread(scrapyengine.stop)

    def _call_and_block_until_signal(self, signal, f, *a, **kw):
        q = Queue.Queue()
        def unblock():
            q.put(None)
        dispatcher.connect(unblock, signal=signal)
        reactor.callFromThread(f, *a, **kw)
        q.get()


def main():
    import os
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'myproject.settings')

    from scrapy.conf import settings
    from scrapy.crawler import CrawlerThread

    settings.overrides['LOG_ENABLED'] = False # avoid log noise

    def item_passed(item):
        print "Just scraped item:", item

    dispatcher.connect(item_passed, signal=signals.item_passed)

    crawler = CrawlerThread()
    print "Starting crawler thread..."
    crawler.start()
    print "Crawling somedomain.com...."
    crawler.crawl('somedomain.com') # blocking call
    print "Crawling anotherdomain.com..."
    crawler.crawl('anotherdomain.com') # blocking call
    print "Stopping crawler thread..."
    crawler.stop()

if __name__ == '__main__':
    main()
