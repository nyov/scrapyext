"""
RobotsTxt - robot exclusion rules parser

This is a middleware to respect robots.txt policies. To activate it you must
enable this middleware and enable the ROBOTSTXT_OBEY setting.

This version uses the robotsexclusionrulesparser,
which can handle GYM2008 wildcards and non-ASCII files.

Also note, this middleware implicitly assumes one spider will crawl one domain
with one robots.txt file.  This may or may not be true for your application.
Using this approach, the robots.txt file is downloaded only once for each spider
type and fewer page requests that violate robots.txt occur

originally imported from
http://snipplr.com/view/67002/robot-exclusion-rules-parser/

# author: kurtjx
# date  : Apr 12, 2011

"""

from robotexclusionrulesparser import RobotExclusionRulesParser

from scrapy.xlib.pydispatch import dispatcher

from scrapy import signals, log
from scrapy.project import crawler
from scrapy.exceptions import NotConfigured, IgnoreRequest
from scrapy.http import Request
from scrapy.utils.httpobj import urlparse_cached
from scrapy.conf import settings

class RobotsTxtMiddleware(object):
    DOWNLOAD_PRIORITY = 1000

    def __init__(self):
        if not settings.getbool('ROBOTSTXT_OBEY'):
            raise NotConfigured

        self._parsers = {}
        self._spider_netlocs = {}
        self._useragents = {}
        dispatcher.connect(self.spider_opened, signals.spider_opened)

    def process_request(self, request, spider):
        if spider.settings.getbool('ROBOTSTXT_OBEY'):
            useragent = self._useragents[spider.name]
            rp = self.robot_parser(request, spider)
            if rp and not rp.is_allowed(useragent, request.url):
                log.msg("Forbidden by robots.txt: %s" % request,
                        level=log.INFO, spider=spider)
                raise IgnoreRequest

    def robot_parser(self, request, spider):
        url = urlparse_cached(request)
        netloc = url.netloc
        if netloc not in self._parsers:
            self._parsers[netloc] = None
            robotsurl = "%s://%s/robots.txt" % (url.scheme, url.netloc)
            robotsreq = Request(robotsurl, priority=self.DOWNLOAD_PRIORITY)
            dfd = crawler.engine.download(robotsreq, spider)
            dfd.addCallback(self._parse_robots)
            self._spider_netlocs[spider.name].add(netloc)
        return self._parsers[netloc]

    def _parse_robots(self, response):
        rp = RobotExclusionRulesParser()
        rp.parse(response.body)
        self._parsers[urlparse_cached(response).netloc] = rp

    def spider_opened(self, spider):
        if not self._spider_netlocs.has_key(spider.name):
            self._spider_netlocs[spider.name] = set()
            self._useragents[spider.name] = spider.settings['USER_AGENT']
