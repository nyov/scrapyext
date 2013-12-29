"""
Sitemap Spider

This script allows you to quickly crawl pages based off of the sitemap(s)
for any given domain.  The XmlXPathSelector was not doing it for me nor
was the XmlItemExporter this code is by far the fastest and easiest way
I have found to crawl a site based off of the URLs listed in the sitemap.

imported from
http://snipplr.com/view/66999/sitemap-spider/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: wynbennett
# date  : Jun 21, 2011
"""
import re

from scrapy.spider import BaseSpider
from scrapy import log
from scrapy.utils.response import body_or_str
from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector

class SitemapSpider(BaseSpider):
	name = "SitemapSpider"
	start_urls = ["http://www.domain.com/sitemap.xml"]

	def parse(self, response):
		nodename = 'loc'
		text = body_or_str(response)
		r = re.compile(r"(<%s[\s>])(.*?)(</%s>)" % (nodename, nodename), re.DOTALL)
		for match in r.finditer(text):
			url = match.group(2)
			yield Request(url, callback=self.parse_page)

	def parse_page(self, response):
                hxs = HtmlXPathSelector(response)

                #Mock Item
		blah = Item()

		#Do all your page parsing and selecting the elemtents you want
                blash.divText = hxs.select('//div/text()').extract()[0]
		yield blah
