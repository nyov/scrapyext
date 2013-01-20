"""
Text-file Spider Queue

imported from
http://snipplr.com/view/67008/textfile-spider-queue/

"""
# Description
# -----------
#
# This is a Spider Queue that uses a plain text file as backend, storing one spider name per line.
#
# Usage example
# -------------
#
# First start scrapy in server mode:
#
#     scrapy runserver --set SPIDER_QUEUE_CLASS=path.to.TextFileSpiderQueue
#
# Then add spiders to crawl from the shell with:
#
#     $ echo myspider >>queue.txt
#
# It also works with the Scrapy `queue` command:
#
#     $ scrapy queue add myspider
#     Added: name=pwc_sg args={}
#
# Limitations
# -----------
#
# * It doesn't support spider arguments
#
# * It's not concurrency safe.

import os

from zope.interface import implements

from scrapy.interfaces import ISpiderQueue

class TextFileSpiderQueue(object):

    implements(ISpiderQueue)

    FILE = 'queue.txt'

    @classmethod
    def from_settings(cls, settings):
        return cls()

    def add(self, name, **spider_args):
        with open(self.FILE, 'a') as f:
            f.write(name + os.linesep)

    def pop(self):
        msgs = list(open(self.FILE)) if os.path.exists(self.FILE) else []
        if not msgs:
            return
        with open(self.FILE, 'w') as f:
            f.writelines(msgs[1:])
        return {'name': msgs[0].strip()}

    def count(self):
        return len(list(open(self.FILE)))

    def list(self):
        return [{'name': x.strip()} for x in open(self.FILE)]

    def clear(self):
        os.remove(self.FILE)

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: pablo
# date  : Sep 05, 2010
