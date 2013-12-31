"""
RSSSpider - gather RSS feeds on a page using feedparser

This is a spider that can crawl RSS feeds in a version independent manner.
It uses Mark pilgrim's excellent feedparser utility to parse RSS feeds.
You can read about the nightmares of  RSS incompatibility [here](http://diveintomark.org/archives/2004/02/04/incompatible-rss)
and  download feedparser that strives to resolve it from [here](http://feedparser.org/docs/)
The scripts processes only certain elements in the feeds(title, link and summary)
The items may be saved in the Item pipeline which I leave to you.

Please let me know about any discrepencies you may find in the technical and functional aspects of this scipt.

-Sid

imported from
http://snipplr.com/view/67003/scrapy-snippet-to-gather-rss-feeds-on-a-pageusing-feedparser/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: itissid
# date  : Feb 20, 2011
"""

import feedparser
import re
import urlparse

from scrapy.spider import BaseSpider

from scrapy.selector import HtmlXPathSelector
from scrapy.selector import XmlXPathSelector
from scrapy.http import Request


class MalformedURLException(Exception):
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return repr(self.value)


class RssFeedItem(Item):
	title = Field()# the Title of the feed
	link = Field()# the URL to the web site(not the feed)
	summary = Field();# short description of feed
	entries = Field();# will contain the RSSEntrItems


class RssEntryItem(RssFeedItem):
	published = Field()


class RSSSpider(BaseSpider):
    name = "rssspider"
    allowed_domain = ["news.google.com"]
    start_urls = [
        "http://news.google.com/"

    ]
    _date_pattern = re.compile( \
        r'(\d{,2})/(\d{,2})/(\d{4}) (\d{,2}):(\d{2}):(\d{2})');
    _http_pattern = re.compile(r'^http:\/\/');
    _gathered_fields = ('published_parsed' ,'title' ,  'link' ,'summary');


    def parse(self, response):
        #recieve Parsed urls here...
        hxs = HtmlXPathSelector(response)
        base_url = response.url;
        res = urlparse.urlparse(base_url);
        self.allowed_domain = [res.netloc];


        print ('**********BASE URL********',base_url);
        links = hxs.select('//a/@href').extract();
        self.num_links = len(links);
        self.num_links_proc = 0;
        print 'Number of links TBP %s'%(self.num_links);
        for url in links:
            #TODO: Inform mongo about progress
            if(self._http_pattern.match(url)):
                # this is an absolute URL
                if url.find(self.allowed_domain[0])!=-1 :
                    try:
                        #callback should be in a separate function. Otherwise all links in this will be crawled too as this function is recursive.
                        yield Request(url, callback=self.first_level_links);
                    except:
                        pass;
                else:
                    # this was an absolute URL but the domain was not the same, so dont crawl
                    pass

            else:
                #relative URL we should try to append the domain and fetch the page
                yield Request(urlparse.urljoin(base_url, url), callback=self.first_level_links);
    # This page will process the first level links
    def first_level_links(self, response):
        print('****First Level links:',response.url);
        r = self.detect_feed(response);
        if r:
            yield r;
        pass
    # detect an RSS Feed and return a RssFeedItem Object
    def detect_feed(self, response):
        """Just detects the feed in the links and returns an Item"""
        xxs = XmlXPathSelector(response);
        '''Need to tweak the feedparser lib to just use the headers from response instead of
        d/l the feed page again, rather than d/l it again
        '''

        if any(xxs.select("/%s" % feed_type) for feed_type in ['rss', 'feed', 'xml', 'rdf']):
            try:
                rssFeed = feedparser.parse(response.url);
                return  self.extract_feed(rssFeed)
            except:
                raise Exception('Exception while parsing/extracting the feed')

        return None

    def extract_feed(self, parsed_feed):
        """
        Takes a feed from the feedparser and returns the constructed items
        """

        if hasattr(parsed_feed.feed, 'link') and (hasattr(parsed_feed.feed,'title')
                or  hasattr(parsed_feed.feed,'description')) and parsed_feed.entries:
            r = RssFeedItem();
            if 'title' in parsed_feed.feed:
                r['title'] = parsed_feed.feed.title;
            if 'subtitle' in parsed_feed.feed:
                r['summary'] = parsed_feed.feed.subtitle
            if 'link' in parsed_feed.feed:
                r['link'] = parsed_feed.feed.link

            # entries gathered as list(s) of key value pairs. Each list is an entry item
            entry_lists= [[
                    {i: entry[i]}  for i in entry if i in self._gathered_fields
                ]for entry in parsed_feed.entries if hasattr(entry,'title') and  hasattr(entry,'link') and hasattr(entry,'summary')
            ]

            for entry_list in entry_lists:
                entry_item = RssEntryItem();

                for entry_dict in entry_list:
                    if r.has_key('entries') == False:
                        r['entries'] = list();

                    if 'published_parsed' in entry_dict:
                        entry_item.update({ 'published':date_handler(entry_dict('published_parsed'))});
                    else:
                        entry_item.update(entry_dict);
                    r['entries'].append(entry_item);
            if r['entries']:
                    return r;
            # if there are no entries return null
        return None;
    def dateHandler(self, dateString):
        """parse a UTC date in MM/DD/YYYY HH:MM:SS format"""
        month, day, year, hour, minute, second = \
            self._date_pattern.search(dateString).groups()
        return (int(year), int(month), int(day), \
                int(hour), int(minute), int(second), 0, 0, 0);
