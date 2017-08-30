# coding=utf-8

from logging import getLogger

LOGGER = getLogger(__name__)


class Db(object):
    """ Database proxy """

    def __init__(self, config):
        self.config = config

        self._backend = None
        self._db_name = None
        self._port = None
        self._host = None
        if 'cache_host' in self.config.get('main'):
            import redis
            self._backend = "redis"
            self._host = self.config_get('cache_host')
            self._port = self.config_get('cache_port') or 6379
            self._db_name = self.config_get('cache_db_name') or 0
            self.db = redis.StrictRedis(host=self._host, port=self._port, db=self._db_name)
            self.set_value = self.db.set
            self.has_value = self.db.exists
            self.remove_value = self.db.delete
            LOGGER.info("Cache initialized")
        else:
            self.set_value = lambda x, y, z: None
            self.has_value = lambda x: None
            self.remove_value = lambda x: None

    def config_get(self, name):
        return self.config.get('main').get(name)

    def get(self, key):
        LOGGER.info("Getting item {} from the cache".format(key))
        return self.db.get(key)

    def get_items(self, requested_key):
        keys = self.db.keys(requested_key)
        return [self.get(key) for key in keys]

    def put(self, key, value, ex=86400):
        LOGGER.info("Saving key {} to cache".format(key))
        self.set_value(key, value, ex)

    def remove(self, key):
        self.remove_value(key)

    def has(self, key):
        LOGGER.info("Checking if code {} is in the cache".format(key))
        return self.has_value(key)


def db_key(tender_id):
    return "{}".format(tender_id)
