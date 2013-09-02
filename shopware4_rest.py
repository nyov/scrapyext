"""
ShopwareRESTPipeline

SHOPWARE_SERVICE_NAME = 'scraperapp' # our user/application name
SHOPWARE_API_BASE = 'http://shopware.local:80/api'
SHOPWARE_TOKEN_KEY = 'alWNAdd7ATcEXOMoInRIDsCwZmDWaXKCT84wl9Ow' # get this from shopware admin

"""

from scrapy import log
from scrapy.conf import settings
from scrapy.exceptions import CloseSpider

from datetime import datetime

from .items import RestItem

from scrapy.utils.serialize import ScrapyJSONEncoder

import requests
from requests.auth import HTTPProxyAuth, HTTPDigestAuth

import json

from twisted.internet import defer, threads


class Fault(Exception):
	"""Some exception.
	"""


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

		self.node = {}

		# shopware minimal default item
		self.default_item = RestItem({
			'taxId': 1,
			#'tax': 19,
			'name': 'nexus',
			'mainDetail': {
				'number': 'nex24',
				'prices': [{
					'customerGroupKey': 'EK',
					'basePrice': 16,
					'price': 20, # shop will add VAT (if configured that way)
				}],
			#	'attribute': {
			#		'supplier_url': 'http://example.net',
			#		'supplierUrl': 'http://example.net',
			#	#	'attr19': 'http://example.net',
			#	},
			},
			'active': True,
			'supplier': 'example com',
			'categories': [
				{'id': 5,},
				{'id': 3,},
			],
			'images': [{
				#'id': '1', ## this one is bugged in shopware (doesnt add image to article)
				#'mediaId': '1',
				# needs deduplication on update
				'link': 'http://shopware.local/templates/_emotion/frontend/_resources/images/logo.jpg',
			}],
			'attribute': {
				'attr19': 'http://example.net',
			},
			'description': 'Some Article',
			'descriptionLong': 'Some Article Description',
		})

	def open_spider(self, spider):
		# TODO: test with async / multiple spiders open
		# (attach session to spider? self.session[spider])
#		token = self.access_token, self.access_token_secret
#		self.session = self.oauth.get_session(token=token)
		self.requests = requests.Session()
		self.requests.auth=HTTPDigestAuth(self.name, self.access_token)

		# category mapping
		self.node[spider] = {}
		self.root = 4 # our! root category # self.SHOPWARE_ROOT_NODE
		r = self.requests.get(
			'%s/categories' % (self.api_url),
			headers=self.request_headers,
			allow_redirects=False,
		)
		if r.status_code != 200:
			raise CloseSpider('API not available')
		resp = json.loads(r.content)
		if resp['success'] != True:
			raise CloseSpider('API returned failure')

		ident = spider.name
		if spider.ident:
			ident = spider.ident

		for cat in resp['data']:
			if cat['parentId'] != self.root:
				continue
			if cat['name'] == ident:
				self.node[spider] = cat

		if 'name' not in self.node[spider] or ident not in self.node[spider]['name']:
			# create spider category node
		#	description = '[SCRAPER LOG] Category created at: %s' % datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
			payload = {'name': ident, 'active': False, 'parentId': self.root}
			r = self.requests.post('%s/categories' % (self.api_url), headers=self.request_headers, data=json.dumps(payload))
			resp = json.loads(r.content)
			r = self.requests.get(resp['data']['location'], headers=self.request_headers)
			resp = json.loads(r.content)
			self.node[spider] = resp['data']

#		print self.node[spider]['id']

#		# starting run...
#		description = '[SCRAPER LOG] Starting crawler run at: %s\n' % datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
#		description += self.api.catalog_category.info(self.node[spider].get('category_id'), 0, ['description']).get('description')
#		self.api.catalog_category.update(self.node[spider].get('category_id'), {'description': description, 'available_sort_by': 'name'})

	def close_spider(self, spider):
		# TODO: test with async / multiple spiders open
	#	self.session.close()

		# finished run
	#	description = self.api.catalog_category.info(self.node[spider].get('category_id'), 0, ['description']).get('description')
	#	description += '\n[SCRAPER] Finished crawler run at: %s' % datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
	#	#description += '\n[SCRAPER] -- Run statistics:\n%s' % crawler_output
	#	self.api.catalog_category.update(self.node[spider].get('category_id'), {'description': description, 'available_sort_by': 'name'})

		if self.node[spider]:
			del self.node[spider]

#	def create_item(self, item, spider):
#	def update_item(self, item, spider):

	def process_item(self, item, spider):
		"""
		Push scraped items into Shopware.
		Do not go over CsvItemExporter,
		do not collect direct-sql speedups.
		"""
		# try insert first. if it fails because it exists (check error msg), do an update on :id
		# (todo) try to extend magento api to allow 'upserts' (POST create _or_ update if exists)
		itemdict = dict(self._get_serialized_fields(self.default_item))
#		itemdict = dict(self._get_serialized_fields(dict(item.items() + self.default_item.items())))
		payload = self.encoder.encode(itemdict)
		print payload

		# set proxies, user-agent?
		try:
			log.msg('API CALL: updating product', spider=spider, level=log.DEBUG)
			r = self.requests.put(
				'%s/articles/%s?useNumberAsId=1' % (self.api_url, itemdict['mainDetail']['number']), # API Endpoint
				headers=self.request_headers,
			#	proxies=proxies,
				allow_redirects=False,
				data=payload,
			)

			resp = r.content
			if r.status_code != 302:
				message = r.status_code
				resp = json.loads(r.content)
				if resp:
					message = resp['message']
				else:
					message = r.content
				raise Fault('{0}: Update failed: {1}'.format(self.__class__.__name__, message))

			log.msg('{0}: Product updated: {1}: {2}'.format(self.__class__.__name__, itemdict['mainDetail']['number'], resp), spider=spider, level=log.INFO)
		except Fault as fault:
			#if fault.faultCode == 620: # wrong
			#	log.msg(fault.faultString, spider=spider, level=log.DEBUG)
			# Update failed: Article by id nex21 not found
			log.msg('API CALL: inserting product', spider=spider, level=log.DEBUG)
			r = self.requests.post(
				'%s/articles?useNumberAsId=1' % (self.api_url), # API Endpoint
				headers=self.request_headers,
			#	proxies=proxies,
				allow_redirects=False,
				data=payload,
			)

			if r.status_code != 201 or json.loads(r.content) == None:
				message = r.status_code

			resp = r.content
			message = 'unknown error'
			resp = json.loads(r.content)
			#if r.status_code == 201:
			if resp['success'] == True:
				data = json.loads(r.content)
				message = resp['data']
			if resp['success'] == False:
				message = resp['message']
				# insert error checking here, Exception on errors

			log.msg('{0}: Product inserted: "{1}" as id {2} at {3}'.format(self.__class__.__name__, itemdict['mainDetail']['number'], resp['data']['id'], resp['data']['location'] ), spider=spider, level=log.INFO)

		# non-blocking async version
	#	f = self.requests.post
	#	d = threads.deferToThread(f,
	#		'%s/articles' % self.api_url, # API Endpoint
	#		headers=self.request_headers,
	#		allow_redirects=False,
	#		data=payload,
	#	) # (function, *args, **kwargs)
	#	d.addCallback(self.persist_test, info)
	#	d.addErrback(log.err, self.__class__.__name__ + '.image_downloaded')

		#r = requests.head(url)
		# messages:
		# {"success":true,"data":{"id":2,"location":"http:\/\/shopware.local\/api\/articles\/2"}}
		# {"success":false,"message":"Resource not found"}
		# {"success":false,"message":"Validation error","errors":["tax: This value should not be blank","mainDetail.number: This value should not be blank"]}
		# {"success":false,"message":"Tax by taxrate 1 not found"}
		# {"success":false,"message":"Errormesage: SQLSTATE[23000]: Integrity constraint violation: 1062 Duplicate entry 'sku' for key 'ordernumber'"}
		# -> retry as PUT (update)
		# {"success":false,"message":"Customer Group by key  not found"}

		return item

	export_empty_fields = False
	fields_to_export = None
	encoding = 'utf-8'

	def _get_serialized_fields(self, item, default_value=None, include_empty=None):
		"""Return the fields to export as an iterable of tuples (name,
		serialized_value)
		"""
		if include_empty is None:
			include_empty = self.export_empty_fields
		if self.fields_to_export is None:
			if include_empty:
				field_iter = item.fields.iterkeys()
			else:
				field_iter = item.iterkeys()
		else:
			if include_empty:
				field_iter = self.fields_to_export
			else:
				nonempty_fields = set(item.keys())
				field_iter = (x for x in self.fields_to_export if x in nonempty_fields)
		for field_name in field_iter:
			if field_name in item:
				field = item.fields[field_name]
				value = self.serialize_field(field, field_name, item[field_name])
			else:
				value = default_value

			yield field_name, value

	def serialize_field(self, field, name, value):
		serializer = field.get('serializer', self._to_str_if_unicode)
		return serializer(value)

	def _to_str_if_unicode(self, value):
		return value.encode(self.encoding) if isinstance(value, unicode) else value
