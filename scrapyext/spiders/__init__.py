# base spider
from .spider import Spider
# scrapy.contrib spiders
from .crawl import CrawlSpider, Rule
from .feed import XMLFeedSpider, CSVFeedSpider
from .init import InitSpider
from .sitemap import SitemapSpider
# extra spiders
from .logout import LogoutSpider
from .parsley import ParsleySpider
# (hardcoded stuff, needs improvements)
#from .rss import RSSSpider
#from .selenium import SeleniumSpider
#from .webdriver import WebDriverSpider
