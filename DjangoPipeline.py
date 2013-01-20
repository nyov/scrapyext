"""
Django and Scrapy without using DjangoItem

imported from
http://snipplr.com/view/66985/django-and-scrapy-without-using-djangoitem/

"""
# # django-admin.py startproject djangoapp
# # Create your django model: django startapp website
# # Edit scrapy settings.py with method to point to Django environment
# # Create a pipeline that accesses Django using the model.save() method

***settings.py***

import os
ITEM_PIPELINES = ['myapp.pipelines.DjangoPipeline']

# http://stackoverflow.com/questions/4271975/access-django-models-inside-of-scrapy
def setup_django_env(path):
    import imp, os
    from django.core.management import setup_environ

    f, filename, desc = imp.find_module('settings', [path])
    project = imp.load_module('settings', f, filename, desc)

    setup_environ(project)


current_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
setup_django_env(os.path.join(current_dir, '../djangoapp/'))

***pipelines.py***
from djangoapp.websites.models import Website
from django.db.utils import IntegrityError

class DjangoPipeline(object):

    def process_item(self, item, spider):
        website = Website(link=item['link'][0],
                created=datetime.datetime.now(),
                )
        try:
          website.save()
        except IntegrityError:
          raise DropItem("Contains duplicate domain: %s" % item['link'][0])
        return item

***djangoapp model***

from django.db import models

class Website(models.Model):
    link = models.CharField(max_length=200, unique=True)
    created = models.DateTimeField('date created')

    def __unicode__(self):
            return u"%s" % self.link

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: redtricycle
# date  : Nov 27, 2011
