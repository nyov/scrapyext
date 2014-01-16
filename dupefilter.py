from scrapy.utils.url import canonicalize_url
from scrapy.dupefilter import RFPDupeFilter

import os
import hashlib
import weakref
import re


_fingerprint_cache = weakref.WeakKeyDictionary()
def request_fingerprint(request, include_headers=None):
	"""
	Return the request fingerprint.

	From a modified request, without modifying the Request.
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
		rewritten_url = re.sub(r'(?:requestid=\d+)', '', rewritten_url)
		fp.update(rewritten_url)
		#parsed_body = re.sub(r'(httpSessionId=.*?\n)', '', request.body)
		#parsed_body = re.sub(r'(scriptSessionId=.*?\n)', '', parsed_body)
		#fp.update(parsed_body or '')
		fp.update(request.body or '')
		# end hack #
		if include_headers:
			for hdr in include_headers:
				if hdr in request.headers:
					fp.update(hdr)
					for v in request.headers.getlist(hdr):
						fp.update(v)
		cache[include_headers] = fp.hexdigest()
	return cache[include_headers]


class DupeFilter(RFPDupeFilter):

	#def __init__(self, path=None):
	#	super(DupeFilter, self).__init__(path)

	def request_seen(self, request):
	#	super(DupeFilter, self).request_seen(request)
		fp = request_fingerprint(request)
		if fp in self.fingerprints:
			return True
		self.fingerprints.add(fp)
		if self.file:
			self.file.write(fp + os.linesep)

