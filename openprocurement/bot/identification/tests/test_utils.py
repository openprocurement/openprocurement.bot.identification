# -*- coding: utf-8 -*-
import os
import time
import subprocess
from unittest import TestCase

from openprocurement.bot.identification.databridge.caching import Db
from redis import StrictRedis

config = {
    "main": {
        "cache_host": "127.0.0.1",
        "cache_port": "16379",
        "cache_db_name": 0
    }
}


class TestUtils(TestCase):
    relative_to = os.path.dirname(__file__)  # crafty line
    redis = None
    redis_process = None
    PORT = 16379
    db = Db(config)

    @classmethod
    def setUpClass(cls):
        cls.redis_process = subprocess.Popen(['redis-server', '--port', str(cls.PORT)])
        time.sleep(0.1)
        cls.redis = StrictRedis(port=cls.PORT)

    @classmethod
    def tearDownClass(cls):
        cls.redis_process.terminate()
        cls.redis_process.wait()

    def tearDown(self):
        self.redis.flushall()

    def test_db_init(self):
        self.assertEqual(self.db._backend, "redis")
        self.assertEqual(self.db._db_name, 0)
        self.assertEqual(self.db._port, "16379")
        self.assertEqual(self.db._host, "127.0.0.1")

    def test_db_get(self):
        self.assertIsNone(self.db.get("111"))
        self.db.put("111", "test data")
        self.assertEqual(self.db.get("111"), "test data")

    def test_db_set(self):
        self.db.put("111", "test data")
        self.assertEqual(self.db.get("111"), "test data")

    def test_db_has(self):
        self.assertFalse(self.db.has("111"))
        self.db.put("111", "test data")
        self.assertTrue(self.db.has("111"))
