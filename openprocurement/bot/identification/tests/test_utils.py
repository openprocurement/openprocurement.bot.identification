# -*- coding: utf-8 -*-
import os
import subprocess
from unittest import TestCase

import time

from mock import MagicMock
from openprocurement.bot.identification.databridge.caching import Db, db_key
from openprocurement.bot.identification.databridge.process_tracker import ProcessTracker
from openprocurement.bot.identification.databridge.utils import item_key, check_412
from redis import StrictRedis
from restkit import ResourceError

config = {
    "main": {
        "cache_host": "127.0.0.1",
        "cache_port": "16379",
        "cache_db_name": 0
    }
}

config_no_redis = {
    "main": {
        "cache_port": "16379",
        "cache_db_name": 0
    }
}


class TestUtils(TestCase):
    __test__ = True

    relative_to = os.path.dirname(__file__)  # crafty line
    redis = None
    redis_process = None
    PORT = 16379

    @classmethod
    def setUpClass(cls):
        cls.redis_process = subprocess.Popen(['redis-server', '--port', str(cls.PORT), '--logfile /dev/null'])
        time.sleep(0.1)
        cls.db = Db(config)
        cls.redis = StrictRedis(port=cls.PORT)

    def setUp(self):
        self.process_tracker = ProcessTracker(self.db)
        self.tender_id = "111"
        self.item_id = "222"
        self.document_id = "333"

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

    def test_db_init_no_redis(self):
        db = Db(config_no_redis)
        self.assertIsNone(db._backend)
        self.assertIsNone(db._db_name, 0)
        self.assertIsNone(db._port)
        self.assertIsNone(db._host)

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

    def test_set_item(self):
        self.assertEqual(self.process_tracker.processing_items, {})
        self.assertEqual(self.process_tracker.tender_documents_to_process, {})
        self.process_tracker.set_item(self.tender_id, self.item_id, 1)
        self.assertEqual(self.process_tracker.processing_items, {item_key(self.tender_id, self.item_id): 1})
        self.assertEqual(self.process_tracker.tender_documents_to_process, {db_key(self.tender_id): 1})

    def test_add_docs_amount_to_tender(self):
        self.assertEqual(self.process_tracker.tender_documents_to_process, {})
        self.process_tracker._add_docs_amount_to_tender(self.tender_id, 2)
        self.assertEqual(self.process_tracker.tender_documents_to_process, {db_key(self.tender_id): 2})
        self.process_tracker._add_docs_amount_to_tender(self.tender_id, 3)
        self.assertEqual(self.process_tracker.tender_documents_to_process, {db_key(self.tender_id): 5})

    def test_remove_docs_amount_from_tender(self):
        self.assertEqual(self.process_tracker.tender_documents_to_process, {})
        self.process_tracker.tender_documents_to_process = {db_key(self.tender_id): 2}
        self.assertEqual(self.process_tracker.tender_documents_to_process, {db_key(self.tender_id): 2})
        self.process_tracker._remove_docs_amount_from_tender(self.tender_id)
        self.assertEqual(self.process_tracker.tender_documents_to_process, {db_key(self.tender_id): 1})
        self.process_tracker._remove_docs_amount_from_tender(self.tender_id)
        self.assertEqual(self.process_tracker.tender_documents_to_process, {})

    def test_check_processing_item(self):
        self.assertEqual(self.process_tracker.processing_items, {})
        self.assertFalse(self.process_tracker.check_processing_item(self.tender_id, self.item_id))
        self.process_tracker.set_item(self.tender_id, self.item_id)
        self.assertTrue(self.process_tracker.check_processing_item(self.tender_id, self.item_id))

    def test_check_processed_item(self):
        self.assertEqual(self.process_tracker.processed_items, {})
        self.assertFalse(self.process_tracker.check_processed_item(self.tender_id, self.item_id))
        self.process_tracker.set_item(self.tender_id, self.item_id)
        self.process_tracker.update_items_and_tender(self.tender_id, self.item_id, self.document_id)
        self.assertTrue(self.process_tracker.check_processed_item(self.tender_id, self.item_id))

    def test_check_processed_tender(self):
        self.assertFalse(self.process_tracker.check_processed_tenders(self.tender_id))
        self.redis.set(self.tender_id, "333")
        self.assertTrue(self.process_tracker.check_processed_tenders(self.tender_id))

    def test_update_processing_items(self):
        self.process_tracker.processing_items = {item_key(self.tender_id, self.item_id): 2}
        self.assertEqual(self.process_tracker.processing_items, {item_key(self.tender_id, self.item_id): 2})
        self.process_tracker._update_processing_items(self.tender_id, self.item_id, self.document_id)
        self.assertEqual(self.process_tracker.processing_items, {item_key(self.tender_id, self.item_id): 1})
        self.process_tracker._update_processing_items(self.tender_id, self.item_id, self.document_id)
        self.assertEqual(self.process_tracker.processing_items, {})

    def test_check_412_function(self):
        func = check_412(MagicMock(side_effect=ResourceError(
            http_code=412, response=MagicMock(headers={'Set-Cookie': 1}))))
        with self.assertRaises(ResourceError):
            func(MagicMock(headers={'Cookie': 1}))
        func = check_412(MagicMock(side_effect=ResourceError(
            http_code=403, response=MagicMock(headers={'Set-Cookie': 1}))))
        with self.assertRaises(ResourceError):
            func(MagicMock(headers={'Cookie': 1}))
        f = check_412(MagicMock(side_effect=[1]))
        self.assertEqual(f(1), 1)
