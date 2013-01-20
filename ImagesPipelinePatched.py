"""
Monkey patch for images pipeline

imported from
http://snipplr.com/view/67005/monkey-patch-for-images-pipeline/

"""
# Small monkey patch for images pipeline to make it works with spider, which crawling Items of different nature (and some of this items doesn't have images)

from scrapy.contrib.pipeline.images import ImagesPipeline

old_item_completed = ImagesPipeline.item_completed

def new_item_completed(self, results, item, info):
    if 'image_urls' in item:
        return old_item_completed(self, results, item, info)
    else:
        return item


if ImagesPipeline.item_completed != new_item_completed:
    ImagesPipeline.item_completed = new_item_completed

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: dchaplinsky
# date  : Nov 26, 2010
