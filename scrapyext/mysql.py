"""
MysqlDBPipeline

"""

from scrapy.exceptions import DropItem
from twisted.internet.threads import deferToThread

import MySQLdb


class MysqlDBPipeline(object):

	def __init__(self):
		pass

	@classmethod
	def from_crawler(cls, crawler):
		o = cls(crawler.stats)
	#	crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
		return o

	def open_spider(self, spider):
		self.connect()

	def connect(self):
		try:
			self.conn = MySQLdb.connect(
				host='localhost',
				port=3306,
				user='user',
				passwd='pwd',
				db='my_db',
			)
		except (AttributeError, MySQLdb.OperationalError), e:
			raise e

	def query(self, sql, params=()):
		try:
			cursor = self.conn.cursor()
			cursor.execute(sql, params)
		except (AttributeError, MySQLdb.OperationalError) as e:
			print 'exception generated during sql connection: ', e
			self.connect()
			cursor = self.conn.cursor()
			cursor.execute(sql, params)
		return cursor

	sql_all = """SELECT * FROM store_meta WHERE clean_name = %s"""
	sql_insert = """INSERT INTO store_meta (clean_name) VALUES (%s)"""
	sql_clean_id = """SELECT clean_id FROM store_meta WHERE clean_name = %s"""
	sql_allstores = """SELECT * FROM all_stores WHERE store_name = %s"""
	sql_insert_allstores = """INSERT INTO all_stores (store_name,clean_id) VALUES (%s,%s)"""
	sql_store_id = """SELECT store_id FROM all_stores WHERE store_name =%s"""

	sql_query_discounts = """SELECT * FROM discounts WHERE mall=%s AND store_id=%s AND bonus=%s AND deal_url=%s"""
	sql_insert_discounts = """INSERT INTO discounts (mall,store_id,bonus,per_action,more_than,up_to,deal_url) VALUES (%s,%s,%s,%s,%s,%s,%s)"""
	sql_query_coupons = """SELECT * FROM crawl_coupons WHERE mall=%s AND clean_id=(SELECT clean_id FROM store_meta WHERE clean_name = %s) AND coupon_code=%s AND coupon_text=%s AND expiry_date=%s"""
	sql_insert_coupons = """INSERT INTO crawl_coupons (mall,clean_id,coupon_code,coupon_text,expiry_date) VALUES (%s, (SELECT clean_id FROM store_meta WHERE clean_name = %s), %s, %s, %s)"""

	def process_item(self, item, spider):
		return deferToThread(self.transaction, item, spider)

	def transaction(self, item, spider):
		# clean_name
		clean_name = ''.join(e for e in item['store'] if e.isalnum()).lower()

		# conditional insertion in store_meta
		curr = self.query(self.sql_all, clean_name)
		if not curr.fetchone():
			self.query(self.sql_insert, clean_name)
			self.conn.commit()

		# getting clean_id
		curr = self.query(self.sql_clean_id, clean_name)
		clean_id = curr.fetchone()

		# conditional insertion in all_stores
		curr = self.query(self.sql_allstores, item['store'])
		if not curr.fetchone():
			self.query(self.sql_insert_allstores, (item['store'], clean_id[0]))
			self.conn.commit()

		# getting store_id
		curr = self.query(self.sql_store_id, item['store'])
		store_id = curr.fetchone()

		if item and not item['is_coupon'] \
				and (item['store'] in ['null', ''] \
				or item['bonus'] in ['null', '']):
			raise DropItem(item)
		# conditional insertion in discounts table
		if item and not item['is_coupon']:
			curr = self.query(self.sql_query_discounts, (
					item['mall'],
					store_id[0],
					item['bonus'],
					item['deal_url']
				)
			)
			if not curr.fetchone():
				self.query(self.sql_insert_discounts, (
					item['mall'],
					store_id[0],
					item['bonus'],
					item['per_action'],
					item['more_than'],
					item['up_to'],
					item['deal_url']),
				)
				self.conn.commit()

		# conditional insertion in crawl_coupons table
		elif spider.name not in COUPONS_LESS_STORE:
			if item['expiration'] is not 'null':
				item['expiration']=datetime.strptime(item['expiration'],'%m/%d/%Y').date()
			curr = self.query(self.sql_query_coupons, (
					item['mall'],
					clean_name,
					item['code'],
					self.conn.escape_string(item['discount']),
					item['expiration']
				)
			)
			if not curr.fetchone():
				self.query(self.sql_insert_coupons, (
						item['mall'],
						clean_name,
						item['code'],
						self.conn.escape_string(item['discount']),
						item['expiration']
					)
				)
				self.conn.commit()
		return item
