"""
TorChangeCircuitMiddleware

DownloaderMiddleware that switches Tor circuits on connection failures.

See RetryMiddleware for configuration.

DOWNLOADER_MIDDLEWARE = {
	'scrapy.contrib.downloadermiddleware.retry.RetryMiddleware': None,
	'scrapylib.torretry.TorChangeCircuitMiddleware': 500,
}

"""
from scrapy.contrib.downloadermiddleware.retry import RetryMiddleware
from scrapy import log
from telnetlib import Telnet
import time


class TorChangeCircuitMiddleware(RetryMiddleware):
	""" DownloaderMiddleware that switches Tor circuits on failures.
	"""

	def _retry(self, request, reason, spider):
		log.msg('Initiating circuit switch.', spider=spider, level=log.INFO)
		conn = Telnet('127.0.0.1', 9051)
		conn.read_until("Escape character is '^]'.", 2)
		conn.write('authenticate ""\r\n')
		conn.read_until("250 OK", 2)
		conn.write("signal newnym\r\n")
		conn.read_until("250 OK", 2)
		conn.write("quit\r\n")
		conn.close()
		time.sleep(3)
		log.msg('Circuit switched.', spider=spider, level=log.INFO)
		return super(TorChangeCircuitMiddleware, self)._retry(request, reason, spider)
