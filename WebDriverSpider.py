"""
rendered javascript with webdrivers

imported from
http://snipplr.com/view/66997/rendered-javascript-with-webdrivers/
"""
# This is a piece of code that use webdrivers to load&render a page with Scrapy and Selenium.
#
# This work is based on the snippets [wynbennett](http://snippets.scrapy.org/users/wynbennett/) [posted here](http://snippets.scrapy.org/snippets/21/) some time ago

from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from myItem.items import myItem
from selenium import webdriver
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile

import time
import pprint

class WebDriverSpider(CrawlSpider):
    name = "WebDriverSpider"
    start_urls = ["http://yourDomain.com/yourUrl.html"]

    rules = (
        Rule(SgmlLinkExtractor(allow=('\.html', ), allow_domains=('yourDomain.com', )), callback='parse_page',follow=False),
        )

    def __init__(self):
        CrawlSpider.__init__(self)
        self.verificationErrors = []
        #create a profile with specific add-ons
        #and do this. Firefox to load it
        profile = FirefoxProfile(profile_directory="/home/yourUser/.mozilla/firefox/selenium/")
        self.selenium = webdriver.Firefox(profile)

    def __del__(self):
        self.selenium.quit()
        print self.verificationErrors
        CrawlSpider.__del__(self)

    def parse_page(self, response):
        #normal scrapy result
        hxs = HtmlXPathSelector(response)
        #webdriver rendered page
        sel = self.selenium
        sel.get(response.url)

        if sel:
            #Wait for javascript to load in Selenium
            time.sleep(2.5)

        #Do some crawling of javascript created content with Selenium
        item = myItem()
        item['url'] = response.url
        item['title'] = hxs.select('//title/text()').extract()


        #something u can do only with webdrivers
        item['thatDiv'] = sel.find_element_by_id("thatDiv")

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: rollsappletree
# date  : Aug 25, 2011
