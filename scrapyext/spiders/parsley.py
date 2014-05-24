"""
ParsleySpider

"Parsley is a simple language for extracting structured data from
web pages. It consists of an powerful Selector Language wrapped
with a JSON Structure that can represent page-wide formatting."

We can get Parsley language site parsers (parselets) from Parselets site.

"Parselets.com is a central repository for user-created APIs to the
web, called Parselets. Parselets are snippets of parsing code
written in a language called Parsley, which is a familiar combination
of CSS, XPath, Regular Expressions, and JSON."
(ann.: it's defunkt.
 http://web.archive.org/web/20100107015258/http://parselets.com/ )

In this example, we integrate Parsley with Scrapy using a new class
of Item, ParsleyItem that defines its fields from a parselet code,
and extend the CrawlSpider to create ParsleySpider that provides a
method to parse a response with a parselet and return a ParsleyItem.

imported from
http://snipplr.com/view/67016/parsley-spider/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: void
# date  : Aug 10, 2010
"""

from pyparsley import PyParsley

from scrapy.item import Item, Field
from .crawl import CrawlSpider


class ParsleyItem(Item):
    def __init__(self, parslet_code, *args, **kwargs):
        for name in parslet_code.keys():
            self.fields[name] = Field()

        super(ParsleyItem, self).__init__(*args, **kwargs)


class ParsleySpider(CrawlSpider):
    parslet_code = {}

    def parse_parsley(self, response):
        parslet = PyParsley(self.parslet_code, output='python')
        return ParsleyItem(self.parslet_code, parslet.parse(string=response.body))


'''
# example youtube.com spider

from scrapy.conf import settings
from scrapy.contrib.spiders import Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor


YOUTUBE_PARSLET = {
    "title": "h1",
    "desc": ".description",
    "rating": ".ratingL @title",
    "embed": "#embed_code @value"
}


class YoutubeSpider(ParsleySpider):
    query = settings.get('QUERY')

    domain_name = 'youtube.com'
    start_urls = ['http://www.youtube.com/results?search_query=%s&page=1' %
                  query]

    rules = (
        Rule(SgmlLinkExtractor(allow=(r'results\?search_query=%s&page=\d+' %
                                      query,))),
        Rule(SgmlLinkExtractor(allow=(r'watch\?v=',),
                               restrict_xpaths=['//div[@id="results-main-content"]']),
             'parse_parsley'),
    )

    parslet_code = YOUTUBE_PARSLET
'''
