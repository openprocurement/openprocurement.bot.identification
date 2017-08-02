# -*- coding: utf-8 -*-
from gevent import monkey
from openprocurement.bot.identification.databridge.utils import ProcessTracker

monkey.patch_all()

import uuid
import unittest
import datetime
from gevent.queue import Queue
from mock import patch, MagicMock
from time import sleep
from munch import munchify
from restkit.errors import (Unauthorized, RequestFailed, ResourceError)

from openprocurement.bot.identification.databridge.scanner import Scanner
from openprocurement.bot.identification.tests.utils import custom_sleep

from openprocurement.bot.identification.databridge.sleep_change_value import APIRateController


class TestScannerWorker(unittest.TestCase):
    def setUp(self):
        self.process_tracker = ProcessTracker(MagicMock(has=MagicMock(return_value=False)))
        self.tenders_id = [uuid.uuid4().hex for _ in range(4)]
        self.sleep_change_value = APIRateController()
        self.client = MagicMock()
        self.tender_queue = Queue(10)
        self.worker = Scanner.spawn(self.client, self.tender_queue, MagicMock(), self.process_tracker,
                                    self.sleep_change_value)

    def tearDown(self):
        self.worker.shutdown()
        del self.worker

    @staticmethod
    def munchify(prev_p, next_p, status, id, procurementMethodType, data=True):
        if data:
            return munchify({'prev_page': {'offset': prev_p},
                             'next_page': {'offset': next_p},
                             'data': [{'status': status,
                                       "id": id,
                                       'procurementMethodType': 'aboveThreshold{}'.format(procurementMethodType)}]})
        else:
            return munchify({'prev_page': {'offset': prev_p},
                             'next_page': {'offset': next_p},
                             'data': []})

    def test_init(self):
        self.assertGreater(datetime.datetime.now().isoformat(), self.worker.start_time.isoformat())
        self.assertEqual(self.worker.tenders_sync_client, self.client)
        self.assertEqual(self.worker.filtered_tender_ids_queue, self.tender_queue)
        self.assertEqual(self.worker.sleep_change_value.time_between_requests, 0)
        self.assertEqual(self.worker.delay, 15)
        self.assertEqual(self.worker.exit, False)

    @patch('gevent.sleep')
    def test_worker(self, gevent_sleep):
        """ Returns tenders, check queue elements after filtering """
        gevent_sleep.side_effect = custom_sleep
        self.client.sync_tenders.side_effect = [
            RequestFailed(),
            # worker must restart
            self.munchify('123', '1234', "active.qualification", self.tenders_id[0], 'UA'),
            Unauthorized(),
            self.munchify('123', '1234', "active.tendering", uuid.uuid4().hex, 'UA'),
            self.munchify('123', '1234', "active.pre-qualification", self.tenders_id[1], 'EU')]
        for tender_id in self.tenders_id[0:2]:
            self.assertEqual(self.tender_queue.get(), tender_id)

    @patch('gevent.sleep')
    def test_429(self, gevent_sleep):
        """Receive 429 status, check queue, check sleep_change_value"""
        gevent_sleep.side_effect = custom_sleep
        self.client.sync_tenders.side_effect = [
            self.munchify('1234', '1235', "active.pre-qualification", self.tenders_id[0], 'EU'),
            self.munchify('1235', '1236', "active.tendering", uuid.uuid4().hex, 'UA'),
            self.munchify('1236', '1237', "active.qualification", self.tenders_id[1], 'UA'),
            ResourceError(http_code=429),
            self.munchify('1237', '1238', "active.qualification", self.tenders_id[2], 'UA')]
        self.sleep_change_value.increment_step = 2
        self.sleep_change_value.decrement_step = 1
        for tender_id in self.tenders_id[0:3]:
            self.assertEqual(self.tender_queue.get(), tender_id)
        self.assertEqual(self.sleep_change_value.time_between_requests, 1)

    @patch('gevent.sleep')
    def test_429_sleep_change_value(self, gevent_sleep):
        """Three times receive 429, check queue, check sleep_change_value"""
        gevent_sleep.side_effect = custom_sleep
        self.client.sync_tenders.side_effect = [
            self.munchify('123', '1234', "active.pre-qualification", self.tenders_id[0], 'EU'),
            self.munchify('123', '1234', "active.tendering", uuid.uuid4().hex, 'UA'),
            self.munchify('123', '1234', "active.tendering", uuid.uuid4().hex, 'UA'),
            self.munchify('123', '1234', "active.tendering", uuid.uuid4().hex, 'UA'),
            ResourceError(http_code=429),
            ResourceError(http_code=429),
            ResourceError(http_code=429),
            self.munchify('123', '1234', "active.pre-qualification", self.tenders_id[1], 'EU')]
        self.sleep_change_value.increment_step = 1
        self.sleep_change_value.decrement_step = 0.5
        for tender_id in self.tenders_id[0:2]:
            self.assertEqual(self.tender_queue.get(), tender_id)
        self.assertEqual(self.sleep_change_value.time_between_requests, 2.5)

    @patch('gevent.sleep')
    def test_backward_dead(self, gevent_sleep):
        """Test when backward dies """
        gevent_sleep.side_effect = custom_sleep
        self.client.sync_tenders.side_effect = [
            self.munchify('123', '1234', "active.pre-qualification", self.tenders_id[0], 'EU'),
            self.munchify('1234', '1235', "active.pre-qualification", self.tenders_id[1], 'EU'),
            ResourceError(http_code=403),
            self.munchify('1235', '1236', None, None, False),
            self.munchify('1236', '1237', "active.pre-qualification", self.tenders_id[2], 'EU')]
        self.sleep_change_value.increment_step = 1
        self.sleep_change_value.decrement_step = 0.5
        for tender_id in self.tenders_id[0:3]:
            self.assertEqual(self.tender_queue.get(), tender_id)

    @patch('gevent.sleep')
    def test_forward_dead(self, gevent_sleep):
        """ Test when forward dies"""
        gevent_sleep.side_effect = custom_sleep
        self.client.sync_tenders.side_effect = [
            self.munchify('1234', '123', None, None, None, False),
            self.munchify('1234', '123', None, None, None, False),
            self.munchify('1234', '1235', "active.pre-qualification", self.tenders_id[0], 'EU'),
            ResourceError(http_code=403),
            self.munchify('1234', '1235', "active.pre-qualification", self.tenders_id[1], 'EU')]
        self.sleep_change_value.increment_step = 1
        self.sleep_change_value.decrement_step = 0.5
        for tender_id in self.tenders_id[0:2]:
            self.assertEqual(self.tender_queue.get(), tender_id)

    @patch('gevent.sleep')
    def test_forward_run(self, gevent_sleep):
        """  Run forward when backward get empty response and
            prev_page.offset is equal to next_page.offset """
        gevent_sleep.side_effect = custom_sleep
        self.client.sync_tenders.side_effect = [
            self.munchify('123', '1234', "active.pre-qualification", self.tenders_id[0], 'EU'),
            self.munchify('1234', '1235', "active.pre-qualification", self.tenders_id[1], 'EU'),
            self.munchify('1235', '1235', None, None, None, False),
            self.munchify('1236', '1237', "active.pre-qualification", self.tenders_id[2], 'EU')]
        self.sleep_change_value.increment_step = 1
        self.sleep_change_value.decrement_step = 0.5
        for tender_id in self.tenders_id[0:3]:
            self.assertEqual(self.tender_queue.get(), tender_id)

    @patch('gevent.sleep')
    def test_get_tenders_exception(self, gevent_sleep):
        """ Catch exception in backward worker and after that put 2 tenders to process.Then catch exception for forward
        and after that put tender to process."""
        gevent_sleep.side_effect = custom_sleep
        self.client.sync_tenders.side_effect = [
            self.munchify('123', '1234', "active.pre-qualification", self.tenders_id[0], 'EU'),
            self.munchify('123', '1234', None, None, None, False),
            self.munchify('123', '1234', "active.pre-qualification", self.tenders_id[1], 'EU'),
            self.munchify('123', '1234', "active.pre-qualification", self.tenders_id[2], 'EU'),
            self.munchify('123', '1234', None, None, None, False),
            self.munchify('1234', None, None, None, None, False),
            self.munchify('1234', '1234', "active.pre-qualification", self.tenders_id[3], 'EU')]
        self.sleep_change_value.increment_step = 1
        self.sleep_change_value.decrement_step = 0.5
        for tender_id in self.tenders_id:
            self.assertEqual(self.tender_queue.get(), tender_id)

    @patch('gevent.sleep')
    def test_resource_error(self, gevent_sleep):
        """Raise Resource error, check queue, check sleep_change_value"""
        gevent_sleep.side_effect = custom_sleep
        self.client.sync_tenders.side_effect = [
            ResourceError(http_code=429),
            self.munchify('1237', '1238', "active.qualification", self.tenders_id[0], 'UA')]
        self.sleep_change_value.increment_step = 2
        self.sleep_change_value.decrement_step = 1
        self.assertEqual(self.tender_queue.get(), self.tenders_id[0])
        self.assertEqual(self.sleep_change_value.time_between_requests, 0)

    @patch('gevent.sleep')
    def test_kill_jobs_with_exception(self, gevent_sleep):
        """Kill job and check Exception"""
        gevent_sleep.side_effect = custom_sleep
        self.sleep_change_value.increment_step = 2
        self.sleep_change_value.decrement_step = 1
        worker = Scanner.spawn(MagicMock(), MagicMock(), MagicMock(), self.process_tracker, self.sleep_change_value)
        sleep(1)
        for job in worker.jobs:
            job.kill(exception=Exception)
        sleep(4)
        self.assertFalse(worker.ready())

    @patch('gevent.sleep')
    def test_forward_exception(self, gevent_sleep):
        """  Run forward when backward get empty response and
            prev_page.offset is equal to next_page.offset """
        gevent_sleep.side_effect = custom_sleep
        self.sleep_change_value.increment_step = 1
        self.sleep_change_value.decrement_step = 0.5
        self.worker.initialize_sync = MagicMock(side_effect=[
            ResourceError(msg=RequestFailed()),
            self.munchify('123', '1234', "active.pre-qualification", self.tenders_id[0], 'EU')])
        self.assertEqual(self.tender_queue.get(), self.tenders_id[0])
        self.assertEqual(self.worker.initialize_sync.call_count, 2)
