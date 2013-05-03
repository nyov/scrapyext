from urlparse import urljoin

from scrapy import log
from scrapy.http import HtmlResponse
from scrapy.utils.response import get_meta_refresh
from scrapy.exceptions import IgnoreRequest, NotConfigured

from scrapy.contrib.downloadermiddleware.redirect import RedirectMiddleware

class FixedRedirectMiddleware(RedirectMiddleware):
    """Handle redirection of requests based on response status and meta-refresh html tag"""

    def process_response(self, request, response, spider):
        if 'dont_redirect' in request.meta:
            return response

        if request.method.upper() == 'HEAD':
            if response.status in [301, 302, 303, 307] and 'Location' in response.headers:
                redirected_url = urljoin(request.url, response.headers['location'])
                redirected = request.replace(url=redirected_url)
                return self._redirect(redirected, request, spider, response.status)
            else:
                return response

        if response.status in [302, 303] and 'Location' in response.headers:
            redirected_url = urljoin(request.url, response.headers['location'])
            redirected = self._redirect_request_using_get(request, redirected_url)
            return self._redirect(redirected, request, spider, response.status)

        if response.status in [301, 307] and 'Location' in response.headers:
            redirected_url = urljoin(request.url, response.headers['location'])
            redirected = request.replace(url=redirected_url)
            return self._redirect(redirected, request, spider, response.status)

        return response

    def _redirect(self, redirected, request, spider, reason):
        ttl = request.meta.setdefault('redirect_ttl', self.max_redirect_times)
        redirects = request.meta.get('redirect_times', 0) + 1

        if ttl and redirects <= self.max_redirect_times:
            redirected.meta['redirect_times'] = redirects
            redirected.meta['redirect_ttl'] = ttl - 1
            redirected.meta['redirect_urls'] = request.meta.get('redirect_urls', []) + \
                [request.url]
            #redirected.dont_filter = request.dont_filter
            redirected.dont_filter = True
            redirected.priority = request.priority + self.priority_adjust
            log.msg(format="Redirecting (%(reason)s) to %(redirected)s from %(request)s",
                    level=log.DEBUG, spider=spider, request=request,
                    redirected=redirected, reason=reason)
            return redirected
        else:
            log.msg(format="Discarding %(request)s: max redirections reached",
                    level=log.DEBUG, spider=spider, request=request)
            raise IgnoreRequest

