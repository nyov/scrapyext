"""
WebDriverSpider - rendered javascript with webdrivers

This is a piece of code that use webdrivers to load&render a page with Scrapy and Selenium.

imported from
http://snipplr.com/view/66997/rendered-javascript-with-webdrivers/

# author: rollsappletree
"""

from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.selector import Selector
from scrapy.http import Request
from project.items import Item

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

    def __init__(self, *a, **kw):
        super(WebDriverSpider, self).__init__(*a, **kw)
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
        sel = Selector(response)
        #webdriver rendered page
        len = self.selenium
        len.get(response.url)

        if len:
            #Wait for javascript to load in Selenium
            time.sleep(2.5)

        #Do some crawling of javascript created content with Selenium
        item = Item()
        item['url'] = response.url
        item['title'] = sel.xpath('//title/text()').extract()


        #something u can do only with webdrivers
        item['thatDiv'] = len.find_element_by_id("thatDiv")
