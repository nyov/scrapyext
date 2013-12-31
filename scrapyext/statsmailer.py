"""
StatsMailer extension sends an email and prints stats to stdout when a engine finishes scraping.

Use STATSMAILER_RCPTS setting to give the recipient mail address
"""

from datetime import datetime

from scrapy import signals
from scrapy.mail import MailSender

from pprint import pprint


class StatsMailer(object):

    def __init__(self, stats, recipients, mail):
        self.stats = stats
        self.recipients = recipients
        self.mail = mail

    @classmethod
    def from_crawler(cls, crawler):
        recipients = crawler.settings.getlist("STATSMAILER_RCPTS")
        mail = MailSender.from_settings(crawler.settings)
        o = cls(crawler.stats, recipients, mail)
        crawler.signals.connect(o.engine_stopped, signal=signals.engine_stopped)
        crawler.signals.connect(o.engine_started, signal=signals.engine_started)
        return o

    def engine_started(self, spider):
        self.start_time = datetime.now()

    def engine_stopped(self, spider):
        now_time = datetime.now()
        stats.set_value('global_start_time', str(self.start_time))
        stats.set_value('global_finish_time', str(now_time))
        stats.set_value('global_total_time', str(now_time - self.start_time))

        pprint(self.stats.get_stats())

        if self.recipients:
            body = "Global stats\n\n"
            body += "\n".join("%-50s : %s" % i for i in self.stats.get_stats().items())
            return self.mail.send(self.recipients, "Scrapy stats", body)
