"""
scrapy-service.tac (Twisted Application Framework)

imported from
http://snipplr.com/view/67011/scrapyservicetac-twisted-application-framework/

"""
# This is a tac file that can be used for launching Scrapy as a service (in the [Twisted Application Framework](http://twistedmatrix.com/documents/current/core/howto/application.html) ) using twistd.
#
# You can start the service with:
#
#     twistd -ny scrapy-service.tac
#
# And then schedule spiders with:
#
#     scrapy queue add myspider

from twisted.application.service import Service, Application
from twisted.python import log as txlog

from scrapy import log
from scrapy.crawler import Crawler
from scrapy.conf import settings

class CrawlerService(Service):

    def startService(self):
        settings.overrides['QUEUE_CLASS'] = settings['SERVER_QUEUE_CLASS']
        self.crawler = Crawler(settings)
        self.crawler.install()
        self.crawler.start()

    def stopService(self):
        return self.crawler.stop()

def get_application(logfile, loglevel=log.DEBUG):
    app = Application("Scrapy")
    app.setComponent(txlog.ILogObserver, \
        log.ScrapyFileLogObserver(open(logfile, 'a'), loglevel).emit)
    CrawlerService().setServiceParent(app)
    return app

application = get_application('scrapy.log')

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: pablo
# date  : Aug 26, 2010
