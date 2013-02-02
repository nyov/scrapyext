"""
Middleware to avoid re-visiting already visited items

imported from
http://snipplr.com/view/67018/middleware-to-avoid-revisiting-already-visited-items/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: pablo
# date  : Aug 10, 2010
"""
# This middleware can be used to avoid re-visiting already visited
# items, which can be useful for speeding up the scraping for
# projects with immutable items, ie. items that, once scraped, don't change.

from scrapy import log
from scrapy.http import Request
from scrapy.item import BaseItem
from scrapy.utils.request import request_fingerprint

from myproject.items import MyItem as Item

class IgnoreVisitedItems(object):
    """Middleware to ignore re-visiting item pages if they were already visited
    before. The requests to be filtered by have a meta['filter_visited'] flag
    enabled and optionally define an id to use for identifying them, which
    defaults the request fingerprint, although you'd want to use the item id,
    if you already have it beforehand to make it more robust.
    """

    FILTER_VISITED = 'filter_visited'
    VISITED_ID = 'visited_id'
    CONTEXT_KEY = 'visited_ids'

    def process_spider_output(self, response, result, spider):
        context = getattr(spider, 'context', {})
        visited_ids = context.setdefault(self.CONTEXT_KEY, {})
        ret = []
        for x in result:
            visited = False
            if isinstance(x, Request):
                if self.FILTER_VISITED in x.meta:
                    visit_id = self._visited_id(x)
                    if visit_id in visited_ids:
                        log.msg("Ignoring already visited: %s" % x.url,
                                level=log.INFO, spider=spider)
                        visited = True
            elif isinstance(x, BaseItem):
                visit_id = self._visited_id(response.request)
                if visit_id:
                    visited_ids[visit_id] = True
                    x['visit_id'] = visit_id
                    x['visit_status'] = 'new'
            if visited:
                ret.append(Item(visit_id=visit_id, visit_status='old'))
            else:
                ret.append(x)
        return ret

    def _visited_id(self, request):
        return request.meta.get(self.VISITED_ID) or request_fingerprint(request)
