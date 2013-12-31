"""
StatsMailer extension sends an email and prints stats to stdout when a engine finishes scraping.

Use STATSMAILER_RCPTS setting to give the recipient mail address

imported from
http://snipplr.com/view/66990/my-approach-to-stats-extension/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: dchaplinsky
# date  : Oct 07, 2011
"""

from scrapy.xlib.pydispatch import dispatcher
from datetime import datetime
from scrapy.stats import stats
from scrapy import signals
from scrapy.mail import MailSender
from scrapy.conf import settings

from pprint import pprint

class StatsMailer(object):
    def __init__(self):
        self.recipients = settings.getlist("STATSMAILER_RCPTS")

        dispatcher.connect(self.engine_stopped, signals.engine_stopped)
        dispatcher.connect(self.engine_started, signals.engine_started)

    def engine_started(self):
        self.start_time = datetime.now()

    def engine_stopped(self):
        now_time = datetime.now()
        stats.set_value('start_time', str(self.start_time))
        stats.set_value('finish_time', str(now_time))
        stats.set_value('total_time', str(now_time - self.start_time))

        if self.recipients:
            mail = MailSender()
            body = "Global stats\n\n"
            body += "\n".join("%-50s : %s" % i for i in stats.get_stats().items())
            mail.send(self.recipients, "Scrapy stats", body)

        pprint(stats.get_stats())
