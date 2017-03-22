# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import uuid
import unittest
import datetime
from gevent.queue import Queue
from mock import patch, MagicMock
from time import sleep
from munch import munchify
from restkit.errors import (
    Unauthorized, RequestFailed, ResourceError
)

from openprocurement.bot.identification.databridge.scanner import Scanner
from openprocurement.bot.identification.tests.utils import custom_sleep


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
        sleep(10)

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
            munchify({'prev_page': {'offset': '1234'},
                      'next_page': {'offset': '1235'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '1235'},
                      'next_page': {'offset': '1236'},
                      'data': [{'status': "active.tendering",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdUA'}]}),
            munchify({'prev_page': {'offset': '1236'},
                      'next_page': {'offset': '1237'},
                      'data': [{'status': "active.qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdUA'}]}),
            ResourceError(http_code=425),
            munchify({'prev_page': {'offset': '1237'},
                      'next_page': {'offset': '1238'},
                      'data': [{'status': "active.qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdUA'}]})]

        worker = Scanner.spawn(client, tender_queue, 2, 1)
        sleep(4)
        # Kill worker
        worker.shutdown()
        del worker
        self.assertEqual(tender_queue.qsize(), 3)
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
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.tendering",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdUA'}]}),
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

    @patch('gevent.sleep')
    def test_backward_dead(self, gevent_sleep):
        """Test when backward dies """
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        client.sync_tenders.side_effect = [
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '1234'},
                      'next_page': {'offset': '1235'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '1235'},
                      'next_page': {'offset': '1236'},
                      'data': []}),
            ResourceError(http_code=403),
            munchify({'prev_page': {'offset': '1236'},
                      'next_page': {'offset': '1237'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]})]

        worker = Scanner.spawn(client, tender_queue, 1, 0.5)
        sleep(7)

        # Kill worker
        worker.shutdown()
        del worker
        self.assertEqual(tender_queue.qsize(), 3)

    @patch('gevent.sleep')
    def test_forward_dead(self, gevent_sleep):
        """ Test when forward dies"""
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        client.sync_tenders.side_effect = [
            munchify({'prev_page': {'offset': '1234'},
                      'next_page': {'offset': '123'},
                      'data': []}),
            munchify({'prev_page': {'offset': '1234'},
                      'next_page': {'offset': '123'},
                      'data': []}),
            munchify({'prev_page': {'offset': '1234'},
                      'next_page': {'offset': '1235'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            ResourceError(http_code=403),
            munchify({'prev_page': {'offset': '1234'},
                      'next_page': {'offset': '1235'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]})]

        worker = Scanner.spawn(client, tender_queue, 1, 0.5)
        sleep(7)

        # Kill worker
        worker.shutdown()
        del worker
        self.assertEqual(tender_queue.qsize(), 2)

    @patch('gevent.sleep')
    def test_forward_run(self, gevent_sleep):
        """  Run forward when backward get empty response and
            prev_page.offset is equal to next_page.offset """
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        client.sync_tenders.side_effect = [
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '1234'},
                      'next_page': {'offset': '1235'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '1235'},
                      'next_page': {'offset': '1235'},
                      'data': []}),
            munchify({'prev_page': {'offset': '1236'},
                      'next_page': {'offset': '1237'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]})]

        worker = Scanner.spawn(client, tender_queue, 1, 0.5)
        sleep(5)

        # Kill worker
        worker.shutdown()
        del worker
        self.assertEqual(tender_queue.qsize(), 3)

    @patch('gevent.sleep')
    def test_get_tenders_exception(self, gevent_sleep):
        """ Catch exception in backward worker and after that put 2 tenders to process.Then catch exception for forward
        and after that put tender to process."""
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        client.sync_tenders.side_effect = [
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': []}),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': []}),
            munchify({'prev_page': {'offset': '1234'},
                      'next_page': {'offset': None},
                      'data': []}),
            munchify({'prev_page': {'offset': '1234'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]})
        ]

        worker = Scanner.spawn(client, tender_queue, 1, 0.5)
        sleep(7)

        # Kill worker
        worker.shutdown()
        del worker
        self.assertEqual(tender_queue.qsize(), 3)

    @patch('gevent.sleep')
    def test_resource_error(self, gevent_sleep):
        """Raise Resource error, check queue, check sleep_change_value"""
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        client.sync_tenders.side_effect = [
            ResourceError(http_code=429),
            munchify({'prev_page': {'offset': '1237'},
                      'next_page': {'offset': '1238'},
                      'data': [{'status': "active.qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdUA'}]})]

        worker = Scanner.spawn(client, tender_queue, 2, 1)
        sleep(4)
        # Kill worker
        worker.shutdown()
        del worker
        self.assertEqual(tender_queue.qsize(), 1)
        self.assertEqual(Scanner.sleep_change_value, 0)
        Scanner.sleep_change_value = 0

    @patch('gevent.sleep')
    def test_kill_jobs_with_exception(self, gevent_sleep):
        """Kill job and check Exception"""
        gevent_sleep.side_effect = custom_sleep
        worker = Scanner.spawn(MagicMock(), MagicMock(), 2, 1)
        sleep(1)
        for job in worker.jobs:
            job.kill(exception=Exception)
        sleep(4)
        self.assertEqual(worker.ready(), False)

