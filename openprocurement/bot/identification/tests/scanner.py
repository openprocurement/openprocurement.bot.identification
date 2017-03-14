# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import uuid
import unittest
import datetime
from gevent.queue import Queue

from openprocurement.bot.identification.databridge.scanner import Scanner
from mock import patch, MagicMock
from time import sleep
from munch import munchify
from restkit.errors import (
    Unauthorized, RequestFailed, ResourceError
)





class TestScannerWorker(unittest.TestCase):

    def test_init(self):
        worker = Scanner.spawn(None, None)
        self.assertGreater(datetime.datetime.now().isoformat(),
                           worker.start_time.isoformat())
        self.assertEqual(worker.tenders_sync_client, None)
        self.assertEqual(worker.filtered_tender_ids_queue, None)
        self.assertEqual(worker.increment_step, 1)
        self.assertEqual(worker.decrement_step, 1)
        self.assertEqual(worker.delay, 15)
        self.assertEqual(worker.exit, False)

        worker.shutdown()
        del worker

    @patch('gevent.sleep')
    def test_worker(self, gevent_sleep):
        """ Returns tenders, check queue elements after filtering """
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        client.sync_tenders.side_effect = [
            RequestFailed(),
            # worker must restart
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdUA'}]}),
            Unauthorized(),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.tendering",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdUA'}]}),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]})
        ]

        worker = Scanner.spawn(client, tender_queue)
        sleep(4)

        # Kill worker
        worker.shutdown()
        del worker

        self.assertEqual(tender_queue.qsize(), 2)

    @patch('gevent.sleep')
    def test_425(self, gevent_sleep):
        """Receive 425 status, check queue, check sleep_change_value"""
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        client.sync_tenders.side_effect = [
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.tendering",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdUA'}]}),
            ResourceError(http_code=425),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdUA'}]})]

        worker = Scanner.spawn(client, tender_queue, 2, 1)
        sleep(4)
        # Kill worker
        worker.shutdown()
        del worker
        self.assertEqual(tender_queue.qsize(), 2)
        self.assertEqual(Scanner.sleep_change_value, 1)
        Scanner.sleep_change_value = 0

    @patch('gevent.sleep')
    def test_425_sleep_change_value(self, gevent_sleep):
        """Three times receive 425, check queue, check sleep_change_value"""
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        client.sync_tenders.side_effect = [
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.tendering",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdUA'}]}),
            ResourceError(http_code=425),
            ResourceError(http_code=425),
            ResourceError(http_code=425),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]})]

        worker = Scanner.spawn(client, tender_queue, 1, 0.5)
        sleep(4)
        self.assertEqual(tender_queue.qsize(), 2)
        self.assertEqual(Scanner.sleep_change_value, 2.5)

        # Kill worker
        worker.shutdown()
        del worker
        Scanner.sleep_change_value = 0





