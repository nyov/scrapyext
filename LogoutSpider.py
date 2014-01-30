from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.exceptions import DontCloseSpider

from scrapy.spider import Spider # scrapy 0.22
from scrapy import log


class LogoutSpider(Spider):

	logout_url = ''

	def start_requests(self):
		dispatcher.connect(self.spider_logout, signal=signals.spider_idle)
		return super(LogoutSpider, self).start_requests()

	logout_done = False
	def spider_logout(self, spider):
		if spider != self: return
		if self.logout_done: return
		self.crawler.engine.schedule(self.logout(), spider)
		raise DontCloseSpider('logout scheduled')

	def logout(self, response=None):
		if response and response.meta.get('logout_sent', None):
			# verify logout?
			if 'Logged out' in response.body:
				self.log('Logout successful.', level=log.INFO)
			return

		self.log('Closing down with logout...', level=log.INFO)
		self.logout_done = True # dont care if this request succeeds
		request = Request(url=self.logout_url, callback=self.logout)
		request.meta['logout_sent'] = True
		return request
