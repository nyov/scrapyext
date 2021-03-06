"""
DropElderMiddleware

DropElderMiddleware drops items older than a given time and date
(21:17:27 7/11/2010). It also makes note of the newest item it sees.
If a page drops any items, then no more requests/urls are yielded,
and scraping stops.

This needs to be Middleware in order to end the scraping on a
per-url basis, since a pipeline sees all items indifferently.

It could possibly be broken in to a more general Middleware that
yields urls conditionally; conditions based on the items yielded by
that scraped page.

Maybe the Middleware could work with existing pipelines, merely
making note of what happened to the items belonging to a particular
page as they passed through the pipeline; although this might
warrant changes to infrastructure?

imported from
http://snipplr.com/view/67004/dropeldermiddleware/

# author: Chris2048
"""

from scrapy import signals


class DropElderMiddleware(object):

    @classmethod
    def from_crawler(cls, crawler):
        o = cls()
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def spider_opened(self, spider):
        # restore lastcheck
        self.lastcheck = datetime(2010, 11, 7, 21, 17, 27)
        self.newest = self.lastcheck

    def spider_closed(self, spider):
        # save lastcheck
        print self.newest

    def process_spider_output(self, response, result, spider):
        reqs = None
        nold = 0
        nnew = 0
        for i in result:
            if isinstance(i, Request):
                reqs = i
            elif i['date'] >= self.lastcheck:
                if i['date'] >= self.newest:
                    self.newest = i['date']
                nnew += 1
                yield i
            else:
                nold += 1
        log.msg("Scraped url: %s" % (response.url,), level=log.INFO)
        log.msg("%i items scraped (%i dropped)" % (nold + nnew, nold), level=log.INFO)
        if not nold:
            yield reqs
