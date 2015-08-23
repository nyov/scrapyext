"""
LogoutMixin for Spider

Example:
	class MySpider(LogoutMixin, Spider):

	Note: observe python multiple inheritance order:
	MyClass > Mixin > SubClass > BaseClass

Usage:
	Set a class parameter "logout_url" to the URL
	to call at spider closing time.

	Override "logout" and/or "verify_logout" to
	customize behaviour.

"""

from scrapy import signals
from pydispatch import dispatcher
from scrapy.exceptions import DontCloseSpider
from scrapy.http import Request


class LogoutMixin(object):

	def __init__(self, *a, **kw):
		super(LogoutMixin, self).__init__(*a, **kw)
		dispatcher.connect(self._spider_logout, signal=signals.spider_idle)

	logged_out = False

	def _spider_logout(self, spider):
		if spider != self: return
		if self.logged_out: return
		request = self.logout()
		if not isinstance(request, Request): return
		self.crawler.engine.schedule(request, spider)
		raise DontCloseSpider('logout scheduled')

	def logout(self):
		"""Request to schedule for logout"""
		if self.logout_url:
			return Request(self.logout_url, callback=self.verify_logout, dont_filter=True)

	def verify_logout(self, response):
		"""Verify a successful logout condition"""
		self.logged_out = True


'''
# Example usage
from scrapy.spiders import Spider

class LogoutSpider(LogoutMixin, Spider):

	logout_url = ''

	def logout(self):
		self.log('Closing down with logout [%s]' % (self.logout_url), level=log.INFO)
		return super(LogoutSpider, self).logout()

	def verify_logout(self, response):
		if 'Logged out' in response.body:
			self.logger.info('Logout successful.')
'''
