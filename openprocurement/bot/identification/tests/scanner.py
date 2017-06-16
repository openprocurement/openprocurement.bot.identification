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
        client = MagicMock()
        tender_queue = Queue(10)
        worker = Scanner.spawn(client, tender_queue)
        self.assertGreater(datetime.datetime.now().isoformat(),
                           worker.start_time.isoformat())
        self.assertEqual(worker.tenders_sync_client, client)
        self.assertEqual(worker.filtered_tender_ids_queue, tender_queue)
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
        tenders_id = [uuid.uuid4().hex, uuid.uuid4().hex]
        client.sync_tenders.side_effect = [
            RequestFailed(),
            # worker must restart
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.qualification",
                                "id": tenders_id[0],
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
                                "id": tenders_id[1],
                                'procurementMethodType': 'aboveThresholdEU'}]})
        ]

        worker = Scanner.spawn(client, tender_queue)

        for tender_id in tenders_id:
            self.assertEqual(tender_queue.get(), tender_id)

        # Kill worker
        worker.shutdown()
        del worker

    @patch('gevent.sleep')
    def test_429(self, gevent_sleep):
        """Receive 429 status, check queue, check sleep_change_value"""
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        tenders_id = [uuid.uuid4().hex for i in range(3)]
        client.sync_tenders.side_effect = [
            munchify({'prev_page': {'offset': '1234'},
                      'next_page': {'offset': '1235'},
                      'data': [{'status': "active.pre-qualification",
                                "id": tenders_id[0],
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '1235'},
                      'next_page': {'offset': '1236'},
                      'data': [{'status': "active.tendering",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdUA'}]}),
            munchify({'prev_page': {'offset': '1236'},
                      'next_page': {'offset': '1237'},
                      'data': [{'status': "active.qualification",
                                "id": tenders_id[1],
                                'procurementMethodType': 'aboveThresholdUA'}]}),
            ResourceError(http_code=429),
            munchify({'prev_page': {'offset': '1237'},
                      'next_page': {'offset': '1238'},
                      'data': [{'status': "active.qualification",
                                "id": tenders_id[2],
                                'procurementMethodType': 'aboveThresholdUA'}]})]

        worker = Scanner.spawn(client, tender_queue, 2, 1)

        for tender_id in tenders_id:
            self.assertEqual(tender_queue.get(), tender_id)

        # Kill worker
        worker.shutdown()
        del worker
        self.assertEqual(Scanner.sleep_change_value, 1)
        Scanner.sleep_change_value = 0

    @patch('gevent.sleep')
    def test_429_sleep_change_value(self, gevent_sleep):
        """Three times receive 429, check queue, check sleep_change_value"""
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        tenders_id = [uuid.uuid4().hex for i in range(2)]
        client.sync_tenders.side_effect = [
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": tenders_id[0],
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
            ResourceError(http_code=429),
            ResourceError(http_code=429),
            ResourceError(http_code=429),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": tenders_id[1],
                                'procurementMethodType': 'aboveThresholdEU'}]})]

        worker = Scanner.spawn(client, tender_queue, 1, 0.5)

        for tender_id in tenders_id:
            self.assertEqual(tender_queue.get(), tender_id)

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
        tenders_id = [uuid.uuid4().hex for i in range(3)]
        client.sync_tenders.side_effect = [
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": tenders_id[0],
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '1234'},
                      'next_page': {'offset': '1235'},
                      'data': [{'status': "active.pre-qualification",
                                "id": tenders_id[1],
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            ResourceError(http_code=403),
            munchify({'prev_page': {'offset': '1235'},
                      'next_page': {'offset': '1236'},
                      'data': []}),
            munchify({'prev_page': {'offset': '1236'},
                      'next_page': {'offset': '1237'},
                      'data': [{'status': "active.pre-qualification",
                                "id": tenders_id[2],
                                'procurementMethodType': 'aboveThresholdEU'}]})]

        worker = Scanner.spawn(client, tender_queue, 1, 0.5)

        for tender_id in tenders_id:
            self.assertEqual(tender_queue.get(), tender_id)

        # Kill worker
        worker.shutdown()
        del worker

    @patch('gevent.sleep')
    def test_forward_dead(self, gevent_sleep):
        """ Test when forward dies"""
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        tenders_id = [uuid.uuid4().hex for i in range(2)]
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
                                "id": tenders_id[0],
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            ResourceError(http_code=403),
            munchify({'prev_page': {'offset': '1234'},
                      'next_page': {'offset': '1235'},
                      'data': [{'status': "active.pre-qualification",
                                "id": tenders_id[1],
                                'procurementMethodType': 'aboveThresholdEU'}]})]

        worker = Scanner.spawn(client, tender_queue, 1, 0.5)

        for tender_id in tenders_id:
            self.assertEqual(tender_queue.get(), tender_id)

        # Kill worker
        worker.shutdown()
        del worker

    @patch('gevent.sleep')
    def test_forward_run(self, gevent_sleep):
        """  Run forward when backward get empty response and
            prev_page.offset is equal to next_page.offset """
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        tenders_id = [uuid.uuid4().hex for i in range(3)]
        client.sync_tenders.side_effect = [
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": tenders_id[0],
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '1234'},
                      'next_page': {'offset': '1235'},
                      'data': [{'status': "active.pre-qualification",
                                "id": tenders_id[1],
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '1235'},
                      'next_page': {'offset': '1235'},
                      'data': []}),
            munchify({'prev_page': {'offset': '1236'},
                      'next_page': {'offset': '1237'},
                      'data': [{'status': "active.pre-qualification",
                                "id": tenders_id[2],
                                'procurementMethodType': 'aboveThresholdEU'}]})]

        worker = Scanner.spawn(client, tender_queue, 1, 0.5)

        for tender_id in tenders_id:
            self.assertEqual(tender_queue.get(), tender_id)

        # Kill worker
        worker.shutdown()
        del worker

    @patch('gevent.sleep')
    def test_get_tenders_exception(self, gevent_sleep):
        """ Catch exception in backward worker and after that put 2 tenders to process.Then catch exception for forward
        and after that put tender to process."""
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        tenders_id = [uuid.uuid4().hex for i in range(4)]
        client.sync_tenders.side_effect = [
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                'procurementMethodType': 'aboveThresholdEU',
                                'id': tenders_id[0]}]}),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': []}),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": tenders_id[1],
                                'procurementMethodType': 'aboveThresholdEU'}]}),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": tenders_id[2],
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
                                "id": tenders_id[3],
                                'procurementMethodType': 'aboveThresholdEU'}]})
        ]

        worker = Scanner.spawn(client, tender_queue, 1, 0.5)

        for tender_id in tenders_id:
            self.assertEqual(tender_queue.get(), tender_id)

        # Kill worker
        worker.shutdown()
        del worker

    @patch('gevent.sleep')
    def test_resource_error(self, gevent_sleep):
        """Raise Resource error, check queue, check sleep_change_value"""
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        tender_id = uuid.uuid4().hex
        client.sync_tenders.side_effect = [
            ResourceError(http_code=429),
            munchify({'prev_page': {'offset': '1237'},
                      'next_page': {'offset': '1238'},
                      'data': [{'status': "active.qualification",
                                "id": tender_id,
                                'procurementMethodType': 'aboveThresholdUA'}]})]

        worker = Scanner.spawn(client, tender_queue, 2, 1)
        self.assertEqual(tender_queue.get(), tender_id)

        # Kill worker
        worker.shutdown()
        del worker
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

    @patch('gevent.sleep')
    def test_forward_exception(self, gevent_sleep):
        """  Run forward when backward get empty response and
            prev_page.offset is equal to next_page.offset """
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        tender_id = uuid.uuid4().hex
        worker = Scanner.spawn(client, tender_queue, 1, 0.5)
        worker.initialize_sync = MagicMock(side_effect=[
            ResourceError(msg=RequestFailed()),
                munchify({'prev_page': {'offset': '123'},
                          'next_page': {'offset': '1234'},
                          'data': [{'status': "active.pre-qualification",
                                    "id": tender_id,
                                    'procurementMethodType': 'aboveThresholdEU'}]})
        ])
        self.assertEqual(tender_queue.get(), tender_id)
        self.assertEqual(worker.initialize_sync.call_count, 2)
        worker.shutdown()
        del worker

