"""
LogoutMixin for Spider

Example:
	class MySpider(LogoutMixin, Spider):

	Note: observe python multiple inheritance order:
	MyClass > Mixin > SubClass > BaseClass

Usage:
	Set a class parameter "logout_url" to the URL
	to call at spider closing time.

	Override "logout" and/or "logout_verify" to
	customize behaviour.

"""

from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.exceptions import DontCloseSpider
from scrapy.http import Request


class LogoutMixin(object):

	def __init__(self, *a, **kw):
		super(LogoutMixin, self).__init__(*a, **kw)
		# start_requests() is the better place for this, but that makes
		# it necessary to call the parent class in a spider override,
		# which is easy to forget.
		dispatcher.connect(self._spider_logout, signal=signals.spider_idle)

	_logout_done = False
	def _spider_logout(self, spider):
		if spider != self: return
		if self._logout_done: return
		request = self.logout()
		if not isinstance(request, Request): return
		self.crawler.engine.schedule(request, spider)
		self._logout_done = True # dont care if this request succeeds
		raise DontCloseSpider('logout scheduled')

	def logout(self):
		"""Request to schedule for logout"""
		if self.logout_url:
			return Request(self.logout_url, callback=self.logout_verify, dont_filter=True)

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
