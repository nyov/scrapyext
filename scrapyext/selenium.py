"""
SeleniumSpider - JavaScript rendering with Scrapy and Selenium RC

Some things to note:
You must have the Python version of Selenium RC installed for this to work,
and you must have set up Selenium properly.  Also this is just a template crawler.
You could get much crazier and more advanced with things but I just wanted
to show the basic idea.  As the code stands now you will be doing two requests
for any given url.  One request is made by Scrapy and the other is made by Selenium.
I am sure there are ways around this so that you could possibly just make Selenium
do the one and only request but I did not bother to implement that and by doing
two requests you get to crawl the page with Scrapy too.

This is quite powerful because now you have the entire rendered DOM available
for you to crawl and you can still use all the nice crawling features in Scrapy.
This will make for slower crawling of course but depending on how much you need
the rendered DOM it might be worth the wait.

imported from
http://snipplr.com/view/66998/rendered-javascript-crawler-with-scrapy-and-selenium-rc/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: wynbennett
# date  : Jun 21, 2011
"""

from selenium import selenium

from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request


class SeleniumSpider(CrawlSpider):
	name = "SeleniumSpider"
	start_urls = ["http://www.domain.com"]

	rules = (
		Rule(SgmlLinkExtractor(allow=('\.html', )), callback='parse_page',follow=True),
	)

	def __init__(self):
		CrawlSpider.__init__(self)
		self.verificationErrors = []
		self.selenium = selenium("localhost", 4444, "*chrome", "http://www.domain.com")
		self.selenium.start()

	def __del__(self):
		self.selenium.stop()
		print self.verificationErrors
		CrawlSpider.__del__(self)

	def parse_page(self, response):
		item = Item()

		hxs = HtmlXPathSelector(response)
		#Do some XPath selection with Scrapy
		hxs.select('//div').extract()

		sel = self.selenium
		sel.open(response.url)

		#Wait for javscript to load in Selenium
		time.sleep(2.5)

		#Do some crawling of javascript created content with Selenium
		sel.get_text("//div")
		yield item
