"""
IgnoreVisitedMiddleware

Downloader middleware to ignore URLs found by previous
LogVisitedMiddleware runs.
"""

import os.path, mmap
from scrapy import log
from scrapy.core import exceptions

class IgnoreVisitedMiddleware(object):
	""" DownloaderMiddleware that ignores any urls found in the visited log at
	startup.
	"""
	def __init__(self):
		fname = 'visited.log'

		if os.path.exists(fname):
			log.msg('mmap visited log %s to ignore requests' % fname, level=log.DEBUG)
			self.visited_map = open(fname, 'r')
			self.visited = mmap.mmap(self.visited_map.fileno(), 0, access=mmap.ACCESS_READ)
		else:
			log.msg('no visited log, all requests will be processed', level=log.DEBUG)
			self.visited = None
			self.visisted_map = None

	def __del__(self):
		if self.visisted:
			self.visited.close()

		if self.visited_map:
			self.visited_map.close()

	def process_request(self, request, spider):
		# TODO: mmap.find seems pretty fast, but may want to see if re.search
		# is faster
		if self.visited and self.visited.find(request.url) > -1:
			log.msg('ignoring already visited url: %s' % request.url, level=log.DEBUG)
			raise exceptions.IgnoreRequest
