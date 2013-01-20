"""
Rendered/interactive javascript with gtk/webkit/jswebkit

imported from
http://snipplr.com/view/66996/renderedinteractive-javascript-with-gtkwebkitjswebkit/
"""
# This is a downloader middleware that can be used to get rendered javascript pages using webkit.
#
# this could be extended to handle form requests and load errors, but this is the bare bones code to get it done.
#
# the advantage over the selenium based approachs I've seen is that it only makes one request and you don't have to set up selenium.

from scrapy.http import Request, FormRequest, HtmlResponse

import gtk
import webkit
import jswebkit

class WebkitDownloader( object ):
    def process_request( self, request, spider ):
        if( type(request) is not FormRequest ):
            webview = webkit.WebView()
            webview.connect( 'load-finished', lambda v,f: gtk.main_quit() )
            webview.load_uri( request.url )
            gtk.main()
            js = jswebkit.JSContext( webview.get_main_frame().get_global_context() )
            renderedBody = str( js.EvaluateScript( 'document.documentElement.innerHTML' ) )
            return HtmlResponse( request.url, body=renderedBody )

# Snippet imported from snippets.scrapy.org (which no longer works)
# author: jdwilson
# date  : Sep 06, 2011
