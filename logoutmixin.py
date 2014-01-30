"""
LogoutMixin for Spider

Example:
	class MySpider(LogoutMixin, Spider):

	Note: observe python multiple inheritance order:
	MyClass > Mixin > SubClass > BaseClass

Usage:
	Set a class parameter "logout_url" to the URL
	to call at spider closing time.

"""

from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.exceptions import DontCloseSpider


class LogoutMixin(object):

	logout_url = None

	def start_requests(self):
		dispatcher.connect(self._spider_logout, signal=signals.spider_idle)
		return super(LogoutMixin, self).start_requests()

	_logout_done = False
	def _spider_logout(self, spider):
		if spider != self: return
		if self._logout_done: return
		if self.logout_url:
			self.crawler.engine.schedule(self.logout(), spider)
			self._logout_done = True # dont care if this request succeeds
			raise DontCloseSpider('logout scheduled')

	def logout(self):
		"""Request to schedule for logout"""
		return Request(self.logout_url, callback=self.logout_verify)

	def logout_verify(self, response):
		"""Verify a successful logout"""
		pass


'''
# Example usage
from scrapy.spider import Spider # scrapy 0.22
from scrapy import log

class LogoutSpider(LogoutMixin, Spider):

	logout_url = ''

	def logout(self):
		self.log('Closing down with logout [%s]' % (self.logout_url), level=log.INFO)
		return super(LogoutSpider, self).logout()

	def logout_verify(self, response):
		if 'Logged out' in response.body:
			self.log('Logout successful.', level=log.INFO)
'''
