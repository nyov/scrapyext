import os
import hashlib
import weakref
import re

from scrapy.utils.url import canonicalize_url

_fingerprint_cache = weakref.WeakKeyDictionary()
def request_fingerprint(request, include_headers=None):
	"""
	Calculate request fingerprint on a modified request.
	"""
	if include_headers:
		include_headers = tuple([h.lower() for h in sorted(include_headers)])
	cache = _fingerprint_cache.setdefault(request, {})
	if include_headers not in cache:
		fp = hashlib.sha1()
		fp.update(request.method)
		# hack here #
		# * filter incrementing 'requestid' from url
		rewritten_url = canonicalize_url(request.url)
		#rewritten_url = re.sub(r'(?:requestid=\d+)', '', rewritten_url)
		fp.update(rewritten_url)
		# * ignore sessionid from xhr post body
		parsed_body = request.body or ''
		#parsed_body = re.sub(r'(httpSessionId=.*?\n)', '', parsed_body)
		#parsed_body = re.sub(r'(scriptSessionId=.*?\n)', '', parsed_body)
		fp.update(parsed_body or '')
		# end hack #
		if include_headers:
			for hdr in include_headers:
				if hdr in request.headers:
					fp.update(hdr)
					for v in request.headers.getlist(hdr):
						fp.update(v)
		cache[include_headers] = fp.hexdigest()
	return cache[include_headers]


#import scrapy.utils.request
#scrapy.utils.request.request_fingerprint = request_fingerprint
#from scrapy.utils.request import request_fingerprint


from scrapy.dupefilter import RFPDupeFilter as _RFPDupeFilter

class RFPDupeFilter(_RFPDupeFilter):

	#def __init__(self, path=None):
	#	super(RFPDupeFilter, self).__init__(path)

	def request_seen(self, request):
	#	super(RFPDupeFilter, self).request_seen(request)
		fp = request_fingerprint(request)
		if fp in self.fingerprints:
			return True
		self.fingerprints.add(fp)
		if self.file:
			self.file.write(fp + os.linesep)


from scrapy.contrib.httpcache import FilesystemCacheStorage as _FilesystemCacheStorage

class FilesystemCacheStorage(_FilesystemCacheStorage):

	def __init__(self, *args, **kwargs):
		super(FilesystemCacheStorage, self).__init__(*args, **kwargs)

	def _get_request_path(self, spider, request):
		key = request_fingerprint(request)
		return os.path.join(self.cachedir, spider.name, key[0:2], key)
