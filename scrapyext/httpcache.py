import os
import hashlib
import weakref
import re

from scrapy.utils.url import canonicalize_url

_fingerprint_cache = weakref.WeakKeyDictionary()
def request_fingerprint(request, include_headers=None):
    """
    Return the request fingerprint.

    Ignore Session IDs for fingerprinting (in POST body)
    """
    if include_headers:
        include_headers = tuple([h.lower() for h in sorted(include_headers)])
    cache = _fingerprint_cache.setdefault(request, {})
    if include_headers not in cache:
        fp = hashlib.sha1()
        fp.update(request.method)
        fp.update(canonicalize_url(request.url))
        # special xhr post body, ignore the sessionid
        parsed_body = re.sub(r'(httpSessionId=.*?\n)', '', request.body)
        parsed_body = re.sub(r'(scriptSessionId=.*?\n)', '', parsed_body)
        fp.update(parsed_body or '')
        if include_headers:
            for hdr in include_headers:
                if hdr in request.headers:
                    fp.update(hdr)
                    for v in request.headers.getlist(hdr):
                        fp.update(v)
        cache[include_headers] = fp.hexdigest()
    return cache[include_headers]


# mock patch?
# (also used by RFPDupeFilter)
#import scrapy.utils.request
#scrapy.utils.request.request_fingerprint = request_fingerprint


from scrapy.contrib.httpcache import FilesystemCacheStorage as _FilesystemCacheStorage
class FilesystemCacheStorage(_FilesystemCacheStorage):

    def __init__(self, *args, **kwargs):
        super(FilesystemCacheStorage, self).__init__(*args, **kwargs)

    def _get_request_path(self, spider, request):
        key = request_fingerprint(request)
        return os.path.join(self.cachedir, spider.name, key[0:2], key)

