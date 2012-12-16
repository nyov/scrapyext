# http://blog.just2us.com/2012/07/setting-up-django-with-scrapy/

# settings.py

def setup_django_env(path):
	import imp, os
	from django.core.management import setup_environ
 
	f, filename, desc = imp.find_module('settings', [path])
	project = imp.load_module('settings', f, filename, desc)       
 
	setup_environ(project)
 
	# Add django project to sys.path
	import sys
	sys.path.append(os.path.abspath(os.path.join(path, os.path.pardir)))
 
setup_django_env('/path/to/django/myproject/myproject/')

# items.py

from scrapy.contrib_exp.djangoitem import DjangoItem
from myapp.models import Poll
 
class PollItem(DjangoItem):
    django_model = Poll

# pipelines.py

from myapp.models import Poll
 
class PollPipeline(object):
 
    def process_item(self, item, spider):
 
    	item.save()
        return item
