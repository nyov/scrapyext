"""
Proxy Authentication in Crawler

imported from
http://snipplr.com/view/66987/proxy-authentication-in-crawler/

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: redtricycle
# date  : Nov 21, 2011
"""
# To authenticate the proxy, you must set the Proxy-Authorization header.
# You *cannot* use the form http://user:pass@proxy:port in request.meta['proxy']

import base64

proxy_ip_port = "123.456.789.10:8888"
proxy_user_pass = "awesome:dude"

request = Request(url, callback=self.parse)

# Set the location of the proxy
request.meta['proxy'] = "http://%s" % proxy_ip_port

# setup basic authentication for the proxy
encoded_user_pass=base64.encodestring(proxy_user_pass)
request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass
