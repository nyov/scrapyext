from scrapy import signals
from scrapy import log
from scrapy.conf import settings
from scrapy.exceptions import DropItem
from twisted.enterprise import adbapi

from project.items import DatabaseItem

import datetime
import time
import MySQLdb.cursors


# MySQL Async Twisted Db Pipeline (http://snippets.scrapy.org/snippets/30/)
class MysqlDBPipeline(object):

    def __init__(self):
        """
        Connect to the database in the pool.
        """
        self.dbpool = adbapi.ConnectionPool('MySQLdb',
            #settings['MYSQLDB_SERVER']
            #settings['MYSQLDB_PORT']
            db = settings['MYSQLDB_DB'],
            user = settings['MYSQLDB_USER'],
            passwd = settings['MYSQLDB_PASS'],
            cursorclass = MySQLdb.cursors.DictCursor,
            charset = 'utf8',
            use_unicode = True
        )

    def process_item(self, item, spider):
        """
        Run db query in thread pool and call :func:`_conditional_insert`.
        We only want to process Items of type `DatabaseItem`.

        :param spider: The spider that created the Item
        :type spider:  spider
        :param item: The Item to process
        :type item: Item
        :returns:  Item
        """
        if isinstance(item, DatabaseItem):
            query = self.dbpool.runInteraction(self._conditional_insert, item)
            query.addErrback(self._database_error, item)

        return item

    def _conditional_insert2(self, tx, item):
        """
        Insert an entry in the `log` table and update the `seller` table,
        if necessary, with the seller's name.

        :param tx: Database cursor
        :type tx:  MySQLdb.cursors.DictCursor
        :param item: The Item to process
        :type item: Item
        """
        tx.execute("SELECT id, name FROM seller WHERE id = %s", (item['url']))
        result = tx.fetchone()
        if result:
            log.msg("Seller already in db: %d, %s, %s, %s" % (result['id'], item['seller_id'], item['seller_name'], result['name']), level=log.DEBUG)
            self.sid = result['id']
            if not item['seller_name']:
                item['seller_name'] = result['name']
                log.msg('Should update the name %s to %s, but not going to do it now.' % (result['name'], item['seller_name']), level=log.DEBUG)
        elif item['seller_name']:
            log.msg("Inserting into seller table: %s, %s" % (item['seller_id'], item['seller_name']), level=log.DEBUG)
            tx.execute(\
                "insert into seller (id, name, logged) "
                "values (%s, %s, %s)",
                (item['seller_id'],
                 item['seller_name'],
                 time.time(),
                ) )

        # add item record in the db
        log.msg("Inserting item: %s" % item, level=log.DEBUG)
        tx.execute("""
            insert into item (
                `seller_idfk`, `batch_id`, `index`, `asin`, `title`, `quantity`, `cond`, `price`
                ) values
                ( %s, %s, %s, %s, %s, %s, %s, %s )
            """, (
                self.sid,
                item['batch_id'],
                item['index'],
                item['asin'],
                item['title'],
                item['qty'],
                item['cond'],
                item['price'],
                ) )

    def _conditional_insert(self, tx, item):
        # create record if doesn't exist.
        # all this block run on it's own thread
        tx.execute("SELECT * FROM products WHERE url = %s", (item['url']))
        result = tx.fetchone()
        if result:
            log.msg("Item already stored in db: %s" % (item, result['name']), level=log.DEBUG)
            # do update statement
            #tx.execute(\
            #    "UPDATE products SET "
            #    "spider=%s, "
            #    "name=%s "
            #    "WHERE url = '%s'",
            #    (item['url'][0], int(time.time()))
            #)
        else:
            query = "insert into products "
            "(`spider`, `name`, `description`, `short_description`, `sku`, `price`, `weight`, `tax_class_id`, `ean`, `availability`, `deliverytime`, `image_link`, `unit`, `manufacturer`, `manufacturer_logo`, `category`, `categoryid`, `attributes`, `msrp`, `tier_price`, `url`, `created`, `updated`) values "
            "( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )",
            (item['spider'], item['name'], item['description'], item['short_description'], item['sku'], item['price'], item['weight'], item['tax_class_id'], item['ean'], item['availability'], item['deliverytime'], item['image_link'], item['unit'], item['manufacturer'], item['manufacturer_logo'], item['category'], item['categoryid'], item['attributes'], item['msrp'], item['tier_price'], item['url'], int(time.time()), int(time.time()))
            log.msg("SQL Debug: %s" % query)
            tx.execute(query)
            log.msg("Item stored in db: %s" % item, level=log.DEBUG)

    def _database_error(self, e, item):
        """
        Log an exception to the Scrapy log buffer.
        """
        log.err(e)
        print "Database error: ", e
