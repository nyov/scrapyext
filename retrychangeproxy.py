"""
RetryChangeProxyMiddleware

DownloaderMiddleware that changes Tor nodes on proxy retry.

DOWNLOADER_MIDDLEWARE = {
	'spider.middlewares.RetryChangeProxyMiddleware': 600,
}

"""
from scrapy.contrib.downloadermiddleware.retry import RetryMiddleware


class RetryChangeProxyMiddleware(RetryMiddleware):
	""" DownloaderMiddleware that changes Tor proxies.
	"""

	def _retry(self, request, reason, spider):
		log.msg('Changing proxy')
		tn = telnetlib.Telnet('127.0.0.1', 9051)
		tn.read_until("Escape character is '^]'.", 2)
		tn.write('AUTHENTICATE "267765"\r\n')
		tn.read_until("250 OK", 2)
		tn.write("signal NEWNYM\r\n")
		tn.read_until("250 OK", 2)
		tn.write("quit\r\n")
		tn.close()
		time.sleep(3)
		log.msg('Proxy changed')
		return RetryMiddleware._retry(self, request, reason, spider)
