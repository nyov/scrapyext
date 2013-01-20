"""
make and filter the log file into a html file

imported from
http://snipplr.com/view/66991/make-and-filter-the-log-file-into-a-html-file/

"""
# this script make the scrapy log files into a html file and generate crawl tree.error be taged red,404 be taged yellow and offsite be taged green.
#
# usage example:
#
# $logview.py logfile
#
# output is t.html

import fileinput, re, os
from collections import defaultdict

header ='''<!DOCTYPE html public "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html>
<head>
<style type="text/css">
	.father{
		background-color:#E7EFFF;
		cursor:pointer;
		margin:1px;
	}
	.add{
		background-color:#A5CBF7;
		text-align:center;
		padding-left:0.5em;
		padding-right:0.5em;
		float:left;
		cursor:pointer;
	}
	.child{
		margin-left:20px;
		display:none;
	}
	.family{
		margin-left:20px;
		display:none;
	}
	.r404{
		background-color:#FFFF99;
	}
	.roffsite{
		background-color:#CCFF99;
	}
	.rerror{
		background-color:pink;
	}
</style>
<script language="javascript">
	function show(e){
		var c = e.parentNode;
		c = c.childNodes;
		for  (var i = 0; i < c.length; i++)
			if (c[i].nodeType == 1 && c[i] !== e) c[i].style.display = 'block';
		e.firstChild.innerHTML = '-';
	}
	function hide(e){
		var c = e.parentNode;
		c = c.childNodes;
		for  (var i = 0; i < c.length; i++)
			if (c[i].nodeType == 1 && c[i] !== e) c[i].style.display = 'none';
		e.firstChild.innerHTML = '+';
	}
	function showhide(){
		var c = this.parentNode.childNodes;
		for (var i = 0; i < c.length; i++){
			if (c[i].nodeType == 1 && c[i] != this){
				if (c[i].style.display == 'block') {
					hide(this);
					break;
				}else {
					show(this);
					break;
				}
			}
		}
	}
	function isMember(element,classname){
		var classes = element.className;
		if (!classes) return false;
		if (classes == classname) return true;

		var whitespace = /\s+/;
		if (!whitespace.test(classes)) return false;

		var c = classes.split(whitespace);
		for (var i = 0; i < c.length; i++){
			if (c[i] == classname) return true;
		}
		return false;
	}
	function getElements(classname, tagname, root){
		if (!root) root = document;
		else if (typeof root == 'string') root = doucment.getElementById(root);
		if (!tagname) tagname = '*';
		var all = root.getElementsByTagName(tagname);
		if (!classname) return all;

		var elements = [];
		for (var i = 0; i < all.length; i++){
			var element = all[i];
			if (isMember(element, classname))
				elements.push(element);
		}
		return elements;
	}
	function init(){
		var f = document.body.childNodes;
		for (var i=0; i < f.length; i++){
			if (f[i].nodeType == 1) {
				f[i].style.display = 'block';
			}
		}
		var f = getElements('father')
		for (var i=0;i<f.length;i++){
			if (f[i].nodeType==1) f[i].onclick = showhide;
		}
	}
	function openall(){
		var t = getElements('father');
		for (var i = 0; i < t.length; i++){
			if (t[i].nodeType == 1) show(t[i]);
		}
	}
	function closeall(){
		var t = getElements('father');
		for (var i = 0; i < t.length; i++){
			if (t[i].nodeType == 1) hide(t[i]);
		}
	}
	window.onload = function(){
		init();
	};
</script>
</head>
<body>
<div class="family">
<button onclick="openall();">expand all</button><button onclick="closeall();">close all</button><div>red:error green:offsite blue:can expand yellow:not find</div>
</div>'''
footer = '''</body></html>'''
content = ''
file = open('t.html','w')

class Crawl:
    def __init__(self,referer = 'None',url = None, status = None):
        self.referer = referer
        self.url = url
        self.status = status
    def error(self,error):
        self.error = error

def print_urls(crawllist, referer):
    global file, header, content, footer
    crawls = crawllist[referer]
    for crawl in crawls:
        if crawl.url in crawllist:
            content = content + '<div class="family"><div class="father"><div class="add">+</div><a href="'+str(crawl.url)+'">' + str(crawl.url) + '</a></div>'
            print_urls(crawllist, crawl.url)
            content = content +'</div>'
        else:
            if crawl.status == None:
                c = ''
            else:
                c = 'r'+crawl.status
            if crawl.error != None:
                content = content +'<div class="child '+c+'"><a href="'+str(crawl.url)+'">'+str(crawl.url)+'</a><div class="errordetail">'+crawl.error+'</div></div>'
            else:
                print crawl.url
                content = content +'<div class="child '+c+'"><a href="'+str(crawl.url)+'">'+str(crawl.url)+'</a></div>'

def main():
    global file, header, content, footer
    crawl_re = re.compile(r'\((.*?)\) <GET (.*?)> \(referer: (.*?)\)')
    offsite = re.compile(r'Filtered offsite .* <GET (.*?)>')
    process_error = 'Error processing'
    spider_error = 'Spider error'
    crawl_start_re = re.compile(r'Scrapy .* started')
    allurls = defaultdict(list)
    currentCrawl = None
    error = None
    crawllist = []
    for l in fileinput.input():
        r = crawl_re.search(l)
        if r:
            if currentCrawl != None:
                currentCrawl.error(error)
                crawllist.append(currentCrawl)
            collect = False
            error = None
            t = Crawl(r.group(3), r.group(2), r.group(1))
            currentCrawl = t
            continue
        r = offsite.search(l)
        if r:
            if currentCrawl != None:
                currentCrawl.error(error)
                crawllist.append(currentCrawl)
            collect = False
            error = None
            print r.groups()
            t = Crawl(url = r.group(1), status='offsite')
            currentCrawl = t
            continue
        r = crawl_start_re.search(l)
        if r:
            if currentCrawl != None:
                currentCrawl.error(error)
                crawllist.append(currentCrawl)
            collect = False
            error = None
            t = Crawl(status='started')
            currentCrawl = t
            continue
        if process_error in l:
            collect = True
            currentCrawl.status = 'error'
            error = l
            continue
        if spider_error in l:
            collect = True
            currentCrawl.status = 'error'
            error = l
            continue
        if collect == True:
            error += l
    start = None
    worklist = []
    for i in crawllist:
        if i.status == 'started':
            #new block
            worklist.append(allurls)
            allurls = defaultdict(list)
        allurls[i.referer] += [i]
    worklist.append(allurls)
    for i in worklist:
        print_urls(i, 'None')
    file.writelines(header+content+footer)

def _test():
    import doctest
    doctest.testmod(verbose=True)

if __name__=='__main__':
    main()

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: outofthink
# date  : Sep 22, 2011
