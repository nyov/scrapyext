"""
Script to print crawl tree of a spider run

imported from
http://snipplr.com/view/66995/script-to-print-crawl-tree-of-a-spider-run/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: pablo
# date  : Sep 15, 2011
"""
# This is a script to print the crawl tree of spider run.
#
# Usage example:
#
#     $ python ctree.py myspider.log
#     None
#       http://www.example.com/start_page1
#         http://www.example.com/second_page
#         http://www.example.com/another_page
#     None
#       http://www.example.com/start_page2
#         http://www.example.com/yet_another_page

#!/usr/bin/env python

import fileinput, re
from collections import defaultdict

def print_urls(allurls, referer, indent=0):
    urls = allurls[referer]
    for url in urls:
        print ' '*indent + referer
        if url in allurls:
            print_urls(allurls, url, indent+2)

def main():
    log_re = re.compile(r'<GET (.*?)> \(referer: (.*?)\)')
    allurls = defaultdict(list)
    for l in fileinput.input():
        m = log_re.search(l)
        if m:
            url, ref = m.groups()
            allurls[ref] += [url]
    print_urls(allurls, 'None')

main()
