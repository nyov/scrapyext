# MySQL pipeline (http://snippets.scrapy.org/snippets/33/)
import datetime
import MySQLdb.cursors
from twisted.enterprise import adbapi

class SQLStorePipeline(object):

    def __init__(self):
        self.dbpool = adbapi.ConnectionPool('MySQLdb',
            db='mydb',
            user='myuser',
            passwd='mypass',
            cursorclass=MySQLdb.cursors.DictCursor,
            charset='utf8',
            use_unicode=True
        )

    def process_item(self, item, spider):
        # run db query in thread pool
        query = self.dbpool.runInteraction(self._conditional_insert, item)
        query.addErrback(self.handle_error)

        return item

    def _conditional_insert(self, tx, item):
        # create record if doesn't exist.
        # all this block run on it's own thread
        tx.execute("select * from websites where link = %s", (item['link'][0], ))
        result = tx.fetchone()
        if result:
            log.msg("Item already stored in db: %s" % item, level=log.DEBUG)
        else:
            tx.execute(\
                "insert into websites (link, created) "
                "values (%s, %s)",
                (item['link'][0],
                 datetime.datetime.now())
            )
            log.msg("Item stored in db: %s" % item, level=log.DEBUG)

    def handle_error(self, e):
        log.err(e)


# MySQL Async Twisted Db Pipeline (http://snippets.scrapy.org/snippets/30/)
import MySQLdb.cursors
from twisted.enterprise import adbapi

class InventoryPipeline(object):

    def __init__(self):
        """
        Connect to the database in the pool.

        .. note:: hardcoded db settings
        """
        self.dbpool = adbapi.ConnectionPool('MySQLdb',
            db='database',
            user='user',
            passwd='password',
            cursorclass=MySQLdb.cursors.DictCursor,
            charset='utf8',
            use_unicode=True
        )

    def process_item(self, spider, item):
        """
        Run db query in thread pool and call :func:`_conditional_insert`.
        We only want to process Items of type `InventoryItem`.

        :param spider: The spider that created the Item
        :type spider:  spider
        :param item: The Item to process
        :type item: Item
        :returns:  Item
        """
        if isinstance(item, InventoryItem):
            query = self.dbpool.runInteraction(self._conditional_insert, item)
            query.addErrback(self._database_error, item)

        return item

    def _conditional_insert(self, tx, item):
        """
        Insert an entry in the `log` table and update the `seller` table,
        if neccissary, with the seller's name.

        :param tx: Database cursor
        :type tx:  MySQLdb.cursors.DictCursor
        :param item: The Item to process
        :type item: Item
        """
        tx.execute("SELECT id, name FROM seller WHERE id = %s", (item['seller_id']))
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

    def _database_error(self, e, item):
        """
        Log an exception to the Scrapy log buffer.
        """
        print "Database error: ", e
