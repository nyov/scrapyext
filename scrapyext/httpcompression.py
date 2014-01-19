from scrapy.contrib.downloadermiddleware.httpcompression import HttpCompressionMiddleware as ScrapyHttpCompressionMiddleware


class HttpCompressionMiddleware(ScrapyHttpCompressionMiddleware):
	"""Patch older HttpCompressionMiddleware headers"""

	def process_request(self, request, spider):
		request.headers.setdefault('Accept-Encoding', 'gzip,deflate')
		return request
