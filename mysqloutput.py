# https://codereview.stackexchange.com/questions/15763/how-to-optimize-a-pipline-for-storing-item-in-mysql
class MysqlOutputPipeline(object):
	def __init__(self):
		dispatcher.connect(self.spider_opened, signals.spider_opened)
		dispatcher.connect(self.spider_closed, signals.spider_closed)

	def connect(self):
		try:
			self.conn = MySQLdb.connect(
				host='some_host',
				user='user',
				passwd='pwd',
				db='my_db',
				port=22)
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

	def spider_opened(self, spider):
		self.connect()

	def process_item(self, item, spider):
		# clean_name
		clean_name = ''.join(e for e in item['store'] if e.isalnum()).lower()

		# conditional insertion in store_meta
		sql = """SELECT * FROM store_meta WHERE clean_name = %s"""
		curr = self.query(sql, clean_name)
		if not curr.fetchone():
			sql = """INSERT INTO store_meta (clean_name) VALUES (%s)"""
			self.query(sql, clean_name)
			self.conn.commit()

		# getting clean_id
		sql = """SELECT clean_id FROM store_meta WHERE clean_name = %s"""
		curr = self.query(sql, clean_name)
		clean_id = curr.fetchone()

		# conditional insertion in all_stores
		sql = """SELECT * FROM all_stores WHERE store_name = %s"""
		curr = self.query(sql, item['store'])
		if not curr.fetchone():
			sql = """INSERT INTO all_stores (store_name,clean_id) VALUES (%s,%s)"""
			self.query(sql, (item['store'], clean_id[0]))
			self.conn.commit()

		# getting store_id
		sql = """SELECT store_id FROM all_stores WHERE store_name =%s"""
		curr = self.query(sql, item['store'])
		store_id = curr.fetchone()

		if item and not item['is_coupon'] \
			and (item['store'] in ['null', ''] \
			or item['bonus'] in ['null', '']):
			raise DropItem(item)
		if item and not item['is_coupon']:# conditional insertion in discounts table
			sql = """SELECT *
				FROM discounts
				WHERE mall=%s
				AND store_id=%s
				AND bonus=%s
				AND deal_url=%s"""
			curr = self.query(
				sql,
				(
					item['mall'],
					store_id[0],
					item['bonus'],
					item['deal_url']
				)
			)
			if not curr.fetchone():
				self.query(
					"INSERT INTO discounts
					 (mall,store_id,bonus,per_action,more_than,up_to,deal_url)
					 VALUES (%s,%s,%s,%s,%s,%s,%s)",
					(item['mall'],
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
				sql = """SELECT *
					FROM crawl_coupons
					WHERE mall=%s
					AND clean_id=(SELECT clean_id FROM store_meta WHERE clean_name = %s)
					AND coupon_code=%s
					AND coupon_text=%s
					AND expiry_date=%s"""
			curr = self.query(
				sql,
				(
					item['mall'],
					clean_name,
					item['code'],
					self.conn.escape_string(item['discount']),
					item['expiration']
				)
			)
			if not curr.fetchone():
					sql = """INSERT INTO crawl_coupons
						 (mall,clean_id,coupon_code,coupon_text,expiry_date)
						 VALUES (
							 %s,
							 (SELECT clean_id FROM store_meta WHERE clean_name = %s),
							 %s,
							 %s,
							 %s
						 )"""
					self.query(
						sql,
						(
							item['mall'],
							clean_name,
							item['code'],
							self.conn.escape_string(item['discount']),
							item['expiration']
						)
					)
					self.conn.commit()
		return item
