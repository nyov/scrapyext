"""
Use a random user agent for each request

You can use this middleware to have a random user agent every request the spider makes.
You can define a user USER_AGEN_LIST in your settings and the spider will chose a random user agent from that list every time.

You will have to disable the default user agent middleware and add this to your settings file.

    DOWNLOADER_MIDDLEWARES = {
        'scrapylib.random_useragent.RandomUserAgentMiddleware': 400,
        'scrapy.contrib.downloadermiddleware.useragent.UserAgentMiddleware': None,
    }

imported from
http://snipplr.com/view/66992/use-a-random-user-agent-for-each-request/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: dushyant
# date  : Sep 16, 2011
"""

import random
from scrapy import log
from project.settings import USER_AGENT_LIST

class RandomUserAgentMiddleware(object):

    def process_request(self, request, spider):
        ua = random.choice(USER_AGENT_LIST)
        if ua:
            request.headers.setdefault('User-Agent', ua)
        #log.msg('>>>> UA %s'%request.headers)
