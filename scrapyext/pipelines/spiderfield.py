# -*- coding: utf-8 -*-

class SpiderFieldPipeline(object):
    """ SpiderFieldPipeline stores the spider name in an item. """

    def process_item(self, item, spider):
        item['spider'] = spider.ident or spider.name
        return item
