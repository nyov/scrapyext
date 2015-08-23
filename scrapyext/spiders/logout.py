from scrapy import signals
from scrapy.exceptions import DontCloseSpider

from scrapy import log
from scrapy.http import Request
from .spider import Spider


class LogoutSpider(Spider):
	"""Spider which can clean up before terminating."""

	logout_url = ''

	def __init__(self, crawler):
		self.crawler = crawler

	@classmethod
	def from_crawler(cls, crawler):
		o = cls(crawler)
		crawler.signals.connect(o._logout, signal=signals.spider_idle)
		return o

	def start_requests(self):
		return super(LogoutSpider, self).start_requests()

	logout_done = False
	def _logout(self, spider):
		if spider != self: return
		if self.logout_done: return
		if not self.logout_url: return
		self.crawler.engine.schedule(self.logout(), spider)
		raise DontCloseSpider('logout scheduled')

	def logout(self, response=None):
		if response and response.meta.get('logout_sent', None):
			# verify logout?
			#if 'Logged out' in response.body:
			#	self.log('Logout successful.', level=log.INFO)
			return

		self.log('Closing down with logout...', level=log.INFO)
		self.logout_done = True # dont care if this request succeeds
		request = Request(url=self.logout_url, callback=self.logout, dont_filter=True)
		request.meta['logout_sent'] = True
		return request
