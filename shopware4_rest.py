"""
ShopwareRESTPipeline

SHOPWARE_SERVICE_NAME = 'scraperapp' # our user/application name
SHOPWARE_API_BASE = 'http://shopware.local:80/api'
SHOPWARE_TOKEN_KEY = 'alWNAdd7ATcEXOMoInRIDsCwZmDWaXKCT84wl9Ow' # get this from shopware admin

"""

from scrapy.http import Request

from scrapy.utils.serialize import ScrapyJSONEncoder

import requests
from requests.auth import HTTPProxyAuth, HTTPDigestAuth


class ShopwareRESTPipeline(object):

	def __init__(self):
		"""
		Connect to Shopware REST Api using HTTP digest authentication.
		We need an ADMIN role with sufficient access to insert articles.
		Shopware4 (german) API Guide: http://wiki.shopware.de/_detail_861_487.html
		"""
		self.name         = settings['SHOPWARE_SERVICE_NAME']
		self.api_url      = settings['SHOPWARE_API_BASE']
		self.access_token = settings['SHOPWARE_TOKEN_KEY']

		self.request_headers = {'Content-Type': 'application/json; charset=utf-8', 'Accept': 'application/json'}
		self.encoder = ScrapyJSONEncoder()

	def open_spider(self, spider):
		# TODO: test with async / multiple spiders open
		# (attach session to spider? self.session[spider])
#		token = self.access_token, self.access_token_secret
#		self.session = self.oauth.get_session(token=token)

	def close_spider(self, spider):
		# TODO: test with async / multiple spiders open
#		self.session.close()

	def process_item(self, item, spider):
		"""
		Push scraped items into Shopware.
		Do not go over CsvItemExporter,
		do not collect direct-sql speedups.
		"""
		# try insert first. if it fails because it exists (check error msg), do an update on :id
		# (todo) try to extend magento api to allow 'upserts' (POST create _or_ update if exists)
		payload = {'test': 'me'}
		payload = self.encode_item(item)

		# set proxies, user-agent?
		r = requests.get(
			'%/products' % self.api_url, # API Endpoint
			headers=self.request_headers,
		#	proxies=proxies,
			auth=HTTPDigestAuth(self.name, self.access_token),
			allow_redirects=False,
			data=payload,
		)
	#	r.status_code # 200 OK
		#r = requests.head(url)

		# read error message: product id/sku/...? already exists -> read id, do PUT on id

		data = r.content
		# insert error checking here, Exception on errors
		print data

		return item
