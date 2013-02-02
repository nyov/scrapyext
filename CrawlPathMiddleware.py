"""
Control the crawl path of a spider

imported from
http://snipplr.com/view/66983/crawlpathmiddleware-easily-control-the-crawl-path-of-a-spider/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: kevinbache
# date  : May 11, 2012
"""
# This is a piece of SpiderMiddleware to control the crawl path of a CrawlSpider-like spider.  It works by dropping
# some links extracted from certain pages.  The idea is to get a bit more fine-grained control than using
# LinkExtractors alone because with this middleware you can set different rules to be applied on different pages.
#
# You do this with a series of regular expressions which define the path that the spider is allowed to take.
# Specifically, you put a list of tuples in your settings.py file of the form:
#    (fromUrlPattern, [allowedToURLs], [denyToURLs]).
#
# The allow / deny mechanism works similarly to the one Scrapy uses in LinkExtractors.  Each tuple says something along
# the lines of:
#
# `   If a link was gathered at a page matching the regex in fromUrlPattern
#      Keep it as long as the link's target URL matches at least one pattern in [allowedToURLs]
#      Unless the link's target URL also matches at least one pattern in [denyToURLs]`
#
# Each 'from' page is handled by the first tuple whose fromUrlPattern matches its URL.
# If no tuple matches the URL for the 'from' page, CrawlPathMiddleware ignores that page and doesn't do drop any of
# its extracted links.
#
# If you leave [allowedToURLs] as either '' or [], it allows all URLs.  This is the same as passing [r'.*'].  This is
# useful if you want to have a deny rule without an allow rule.
#
# If you leave [allowedToURLs] as None, it doesn't allow any URLs.  This is the same as passing something like [r'a^']
# and is useful if you want to designate a certain page as a dead end.
#
# If you leave [denyToURLs] as either '', [], or None, it doesn't deny any URLs.  This is the same as passing something
# like [r'a^'] and is useful if you want to have an allow rule without a deny rule.
#
# You can also provide a string containing a single regex instead of a list of regexes for [allowedToURLs] or
# [denyToURLs].  For example, [r'.*my_regex.*'] and r'.*my_regex.*' do the same thing for [allowedToURLs] and
# [denyToURLs].
#
# See the settings.py code for examples.

# =======================
# ===== settings.py =====
# =======================

SPIDER_MIDDLEWARES = {
    'path.to.crawlpath_module.CrawlPathMiddleware': 550,
}

# regular expression patterns for defining url types in shorthand
type1 = r'.*this_regex_uniquely_selects_urls_from_page_type_1.*'
type2 = r'.*this_regex_uniquely_selects_urls_from_page_type_2.*'
type3 = r'.*this_regex_uniquely_selects_urls_from_page_type_3.*'
type4 = r'.*this_regex_uniquely_selects_urls_from_page_type_4.*'

# list of crawl paths for CrawlPathMiddleware
# each crawl path is defined by a tuple comprised of three elements:
#   fromUrlPattern:  a single regular expression (regex) defining pages where this crawl path apples
#                    that is, fromUrlPattern identifies candidate 'from' pages
#   [allowPatterns]: regex or list of regexes defining urls the spider can crawl to from this
#                    'from' page
#   [denyPatterns]:  regex or list of regexes defining urls the spider can't crawl to from this
#                    'from' page
PATH_TUPLES = [
    # these should take the form:
    # (fromUrlPattern, [allowPatterns], [denyPatterns])

    # type2 pages can only go to any type2 or type3 pages
    (type1,     [type2, type3],     ''),

    # type2 pages can only go to type2 pages which don't match either of the bad patterns
    (type2,     type2,              [r'.*bad_pattern_1.*', r'.*bad_pattern_2.*']),

    # type3 pages can go anywhere except type1 pages
    (type3,     '',                 type1),

    # type4 pages can't go anywhere
    (type4,     None,               ''),
]

# If you set PATH_DEBUG to True, CrawlPathMiddleware will log information about
# which links were allowed / denied and why
PATH_DEBUG = True

# This setting controls how wide the printed URLs should be in the logged DEBUG statements.
# This number is only about making the log pretty and readable.  Adjust it as you like.
PATH_DEBUG_URL_LENGTH = 95



# ========================
# ===== crawlpath.py =====
# ========================

# This is a piece of SpiderMiddleware to control the crawl path of a CrawlSpider-like spider.  It works by dropping
# some links extracted from certain pages.  The idea is to get a bit more fine-grained control than using
# LinkExtractors alone because with this middleware you can set different rules to be applied on different pages.
#
# You do this with a series of regular expressions which define the path that the spider is allowed to take.
# Specifically, you put a list of tuples in your settings.py file of the form:
#   (fromUrlPattern, [allowedToURLs], [denyToURLs]).
#
# The allow / deny mechanism works similarly to the one Scrapy uses in LinkExtractors.  Each tuple says something along
# the lines of:
#
#   If a link was gathered at a page matching the regex in fromUrlPattern
#     Keep it as long as the link's target URL matches at least one pattern in [allowedToURLs]
#     Unless the link's target URL also matches at least one pattern in [denyToURLs]
#
# Each 'from' page is handled by the first tuple whose fromUrlPattern matches its URL.
# If no tuple matches the URL for the 'from' page, CrawlPathMiddleware ignores that page and doesn't do drop any of
# its extracted links.
#
# If you leave [allowedToURLs] as either '' or [], it allows all URLs.  This is the same as passing [r'.*'].  This is
# useful if you want to have a deny rule without an allow rule.
#
# If you leave [allowedToURLs] as None, it doesn't allow any URLs.  This is the same as passing something like [r'a^']
# and is useful if you want to designate a certain page as a dead end.
#
# If you leave [denyToURLs] as either '', [], or None, it doesn't deny any URLs.  This is the same as passing something
# like [r'a^'] and is useful if you want to have an allow rule without a deny rule.
#
# You can also provide a string containing a single regex instead of a list of regexes for [allowedToURLs] or
# [denyToURLs].  For example, [r'.*my_regex.*'] and r'.*my_regex.*' do the same thing for [allowedToURLs] and
# [denyToURLs].
#
# See the settings.py code for examples.

from scrapy.http.request import Request
import re
from scrapy import log

class CrawlPathMiddleware(object):
    """SpiderMiddleware to shape the crawl path of a CrawlSpider-like spider using PATH_TUPLES defined in settings.py"""

    def __init__(self, path_tuples, debug, debug_url_length):
        self.DEBUG = debug
        self.DEBUG_URL_LENGTH = debug_url_length
        self.FIRST_MSG_LEN = len("Didn't match 'allow' patterns for") + 2 # not set in any setting

        # path_tuples gets split up into three separate variables
        #   self.fromPats is a list of patterns to match fromUrls
        #   self.allowPatGroups is a list of lists of allow patterns
        #   self.denyPatGroups is a list of lists of deny patterns
        self.fromPats =  [re.compile(t[0]) for t in path_tuples] # patterns to match against each fromURL
        allowPatGroups = [t[1] for t in path_tuples] # allow patterns to match against each toURL
        denyPatGroups =  [t[2] for t in path_tuples] # deny patterns to match against each toURL

        # process and compile allow patterns
        self.allowPatGroups = []
        for pat_group in allowPatGroups:
            if pat_group == '' or pat_group == []:
                # blank allowPats ==> allow everything
                # this will always match as the input string which will evaluate to True (unless input string is '')
                pat_group = '.*'
            elif pat_group is None:
                # None allowPats ==> match nothing
                pat_group = 'a^'

            # compile all patterns in the group
            if isinstance(pat_group, (str, unicode)):
                pats_compiled = [re.compile(pat_group)]
            else:
                pats_compiled = [re.compile(pat) for pat in pat_group]
            self.allowPatGroups.append(pats_compiled)

        # process and compile deny patterns
        self.denyPatGroups = []
        for pat_group in denyPatGroups:
            # blank or None denyPats ==> deny nothing
            if pat_group == '' or pat_group == [] or pat_group is None:
                # this compiles without a problem and won't match anything.  tries to match "a before line start"
                pat_group = r'a^'

            # compile all patterns in the group
            if isinstance(pat_group, (str, unicode)):
                pats_compiled = [re.compile(pat_group)]
            else:
                pats_compiled = [re.compile(pat) for pat in pat_group]
            self.denyPatGroups.append(pats_compiled)

    @classmethod
    def from_settings(cls, settings):
        path_tuples = settings.getlist('PATH_TUPLES')
        debug = settings.getbool('PATH_DEBUG', default=False)
        debug_url_length = settings.getint('PATH_DEBUG_URL_LENGTH', default=90)
        return cls(path_tuples, debug, debug_url_length)

    def _firstIndex(self, myIterable):
        """find the index of the first element in myIterable which evaluates to True"""
        for (counter, option) in enumerate(myIterable):
            if option: return counter
        return None

    def log(self, message, spider, level=log.DEBUG):
        """Log the given messages at the given log level.  Stolen from BaseSpider."""
        # prepend the name of this class to message
        message = '[' + self.__class__.__name__ + '] ' + message
        log.msg(message, spider=spider, level=level)

    def process_spider_output(self, response, result, spider):
        fromUrl = response.url

        # figure out which tuple should handle links from this fromUrl
        fromMatches = [re.match(p, fromUrl) for p in self.fromPats]
        tupleIndex = self._firstIndex(fromMatches)

        if tupleIndex is None:
            # fromUrl didn't match any pattern in fromPats. don't change anything.
            if self.DEBUG: self.log('No matching fromUrl pattern for'.ljust(self.FIRST_MSG_LEN) + \
                                    fromUrl.ljust(self.DEBUG_URL_LENGTH), spider=spider)
            for r in result: yield r
        else:
            # get the allow and deny patterns from the proper tuple
            allowPats = self.allowPatGroups[tupleIndex]
            denyPats = self.denyPatGroups[tupleIndex]

            # check each result element against the allow and deny patterns for the appropriate tuple
            for r in result:
                if isinstance(r, Request):
                    toUrl = r.url

                    allowMatches = [re.match(p, toUrl) for p in allowPats]
                    if any(allowMatches):
                        denyMatches = [re.match(p, toUrl) for p in denyPats]
                        if not any(denyMatches):
                            # toUrl matched an allow pattern and no deny patterns.  allow it to pass.
                            if self.DEBUG: self.log('All ok for'.ljust(self.FIRST_MSG_LEN) + \
                                                    fromUrl.ljust(self.DEBUG_URL_LENGTH) + ' linking to'.ljust(14)+ \
                                                    toUrl, spider=spider)
                            yield r
                        else:
                            # toUrl matched a deny pattern.  drop it.
                            if self.DEBUG: self.log('Matched deny for'.ljust(self.FIRST_MSG_LEN) + \
                                                    fromUrl.ljust(self.DEBUG_URL_LENGTH) + ' linking to'.ljust(14) + \
                                                    toUrl, spider=spider)
                            yield None
                    else:
                        # toUrl didn't match any of the allow patterns,  drop it
                        if self.DEBUG: self.log("Didn't match 'allow' patterns for".ljust(self.FIRST_MSG_LEN) + \
                                                fromUrl.ljust(self.DEBUG_URL_LENGTH) + ' linking to'.ljust(14) + \
                                                toUrl, spider=spider)
                        yield None
                else:
                    # r is an Item.  allow it to pass.
                    yield r
