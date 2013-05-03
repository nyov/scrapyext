"""
MagentoRESTPipeline

MAGENTO_SERVICE_NAME = 'scraperapp' # our application name
MAGENTO_HOST	   = 'http://127.0.0.1:80'
MAGENTO_REQUEST_TOKEN_URL = '%s/oauth/initiate' % MAGENTO_HOST
MAGENTO_AUTHORIZE_URL = '%s/admin/oauth_authorize' % MAGENTO_HOST
MAGENTO_ACCESS_TOKEN_URL = '%s/oauth/token' % MAGENTO_HOST
MAGENTO_API_BASE   = '%s/api/rest/' % MAGENTO_HOST
MAGENTO_TOKEN_KEY  = 'vygdq11yzpecxqwupn1u4zwlamsrpomi' # get this from magento admin
MAGENTO_TOKEN_SEC  = '5x5idvnc8rh4vc8lrxeg4avpge0u63dt' # get this from magento admin
MAGENTO_ACCESS_KEY = 'm1cq8zln3fcmxafl78xizurrkpny8zrw' # get this manually first
MAGENTO_ACCESS_SEC = 'l77q9crhx4igw8f9eq7k86nii9gv7snf' # get this manually first

"""

from scrapy.utils.serialize import ScrapyJSONEncoder
from rauth.service import OAuth1Service


class MagentoRESTPipeline(object):

	def __init__(self, settings):
		"""
		Connect to Magento REST Api using OAuth 1.0 authentication.
		We need an ADMIN role with sufficient access to insert articles.
		Magento API Guide: http://www.magentocommerce.com/api/rest/introduction.html
		"""
		self.name                = settings.get('MAGENTO_SERVICE_NAME')
		self.api_url             = settings.get('MAGENTO_API_BASE')
		self.request_token_url   = settings.get('MAGENTO_REQUEST_TOKEN_URL')
		self.authorize_url       = settings.get('MAGENTO_AUTHORIZE_URL')
		self.access_token_url    = settings.get('MAGENTO_ACCESS_TOKEN_URL')
		self.consumer_key        = settings.get('MAGENTO_TOKEN_KEY')
		self.consumer_secret     = settings.get('MAGENTO_TOKEN_SEC')
		self.access_token        = settings.get('MAGENTO_ACCESS_KEY')
		self.access_token_secret = settings.get('MAGENTO_ACCESS_SEC')

		self.oauth = OAuth1Service(
			name = self.name,
			consumer_key = self.consumer_key,
			consumer_secret = self.consumer_secret,
			request_token_url = self.request_token_url,
			authorize_url = self.authorize_url,
			access_token_url = self.access_token_url,
			base_url = self.api_url,
		)

		self.request_headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
		self.encoder = ScrapyJSONEncoder()

	def generate_access_token(self):
		""" Generate access tokens for a session when we must. """

		# initial handshake to get request token
		request_token, request_token_secret = self.oauth.get_request_token(method='POST', params={'oauth_callback': 'oob'})

		#print 'Our request token is: ' + request_token
		#print '     token secret is: ' + request_token_secret
		#print

		authorize_url = self.oauth.get_authorize_url(request_token)

		print 'Visit this URL in your browser: ' + authorize_url
		code = raw_input('Enter Code from browser: ')

		# get access token for our application
		access_token, access_token_secret = self.oauth.get_access_token(
			request_token=request_token,
			request_token_secret=request_token_secret,
			method='POST',
			data={'oauth_verifier': code}
		)

		print 'Our access token is: ' + access_token
		print '	   token secret is: ' + access_token_secret
		##### > store these in settings file for continued use

	def open_spider(self, spider):
		# TODO: test with async / multiple spiders open
		# (attach session to spider? self.session[spider])
		token = self.access_token, self.access_token_secret
		self.session = self.oauth.get_session(token=token)

	def close_spider(self, spider):
		# TODO: test with async / multiple spiders open
		self.session.close()

	def process_item(self, item, spider):
		"""
		Push scraped items into Magento.
		Do not go over CsvItemExporter,
		do not collect direct-sql speedups.
		(for performance try CSV Feed Exports + Magmi DB updates)
		"""
		# try insert first. if it fails because it exists (check error msg), do an update on :id
		# (todo) try to extend magento api to allow 'upserts' (POST create _or_ update if exists)
		payload = {'test': 'me'}
		payload = self.encode_item(item)
		r = self.session.post(
			'products', # API Endpoint
			header_auth=True,
			headers=self.request_headers,
			allow_redirects=True,
			data=payload,
		)
		# read error message: product id/sku/...? already exists -> read id, do PUT on id

		data = r.content
		# insert error checking here, Exception on errors
		print data

		return item
