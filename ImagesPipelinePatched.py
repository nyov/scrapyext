"""
ImagesPipeline

Monkey-patch for ImagesPipeline, to make it work with spiders,
which crawl Items of different natures, where some of the items
don't have images.

imported from
http://snipplr.com/view/67005/monkey-patch-for-images-pipeline/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: dchaplinsky
# date  : Nov 26, 2010
"""

from scrapy.contrib.pipeline.images import ImagesPipeline

old_item_completed = ImagesPipeline.item_completed

def new_item_completed(self, results, item, info):
    if 'image_urls' in item:
        return old_item_completed(self, results, item, info)
    else:
        return item


if ImagesPipeline.item_completed != new_item_completed:
    ImagesPipeline.item_completed = new_item_completed
