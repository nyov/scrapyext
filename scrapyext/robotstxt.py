"""
RobotsTxt - robot exclusion rules parser

This is a middleware to respect robots.txt policies. To activate it you must
enable this middleware and enable the ROBOTSTXT_OBEY setting.

This version uses the robotsexclusionrulesparser,
which can handle GYM2008 wildcards and non-ASCII files.

"""

from robotexclusionrulesparser import RobotExclusionRulesParser

from scrapy import signals, log
from scrapy.exceptions import NotConfigured, IgnoreRequest
from scrapy.http import Request
from scrapy.utils.httpobj import urlparse_cached


class RobotsTxtMiddleware(object):
    DOWNLOAD_PRIORITY = 1000

    def __init__(self, crawler):
        if not crawler.settings.getbool('ROBOTSTXT_OBEY'):
            raise NotConfigured

        self.crawler = crawler
        self._useragent = crawler.settings.get('USER_AGENT')
        self._parsers = {}
        self._spider_netlocs = set()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_request(self, request, spider):
        useragent = self._useragent
        rp = self.robot_parser(request, spider)
        if rp and not rp.is_allowed(useragent, request.url):
            log.msg(format="Forbidden by robots.txt: %(request)s",
                    level=log.INFO, request=request)
            raise IgnoreRequest

    def robot_parser(self, request, spider):
        url = urlparse_cached(request)
        netloc = url.netloc
        if netloc not in self._parsers:
            self._parsers[netloc] = None
            robotsurl = "%s://%s/robots.txt" % (url.scheme, url.netloc)
            robotsreq = Request(robotsurl, priority=self.DOWNLOAD_PRIORITY)
            dfd = self.crawler.engine.download(robotsreq, spider)
            dfd.addCallback(self._parse_robots)
            self._spider_netlocs.add(netloc)
        return self._parsers[netloc]

    def _parse_robots(self, response):
        rp = RobotExclusionRulesParser()
        rp.parse(response.body)
        self._parsers[urlparse_cached(response).netloc] = rp
