"""
Redirect Scrapy log messages to standard Python logger

imported from
http://snipplr.com/view/67000/redirect-scrapy-log-messages-to-standard-python-logger/

"""
# Redirect Scrapy log messages to standard Python logger

## Add the following lines to your Scrapy project's settings.py file
## This will redirect *all* Scrapy logs to your standard Python logging facility

from twisted.python import log
observer = log.PythonLoggingObserver()
observer.start()

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: saidimu
# date  : Jun 14, 2011
