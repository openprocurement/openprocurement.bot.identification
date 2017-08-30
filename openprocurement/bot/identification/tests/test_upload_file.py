# -*- coding: utf-8 -*-
from gevent import monkey, event

monkey.patch_all()

import uuid
import unittest
import datetime
import requests_mock
from gevent.queue import Queue
from gevent.hub import LoopExit
from time import sleep
from mock import patch, MagicMock

from openprocurement.bot.identification.client import DocServiceClient
from openprocurement.bot.identification.databridge.upload_file_to_doc_service import UploadFileToDocService
from openprocurement.bot.identification.databridge.utils import generate_doc_id, item_key
from openprocurement.bot.identification.databridge.process_tracker import ProcessTracker
from openprocurement.bot.identification.databridge.data import Data
from openprocurement.bot.identification.tests.utils import custom_sleep, generate_answers, AlmostAlwaysFalse
from openprocurement.bot.identification.databridge.constants import file_name, DOC_TYPE
from openprocurement.bot.identification.databridge.sleep_change_value import APIRateController


class TestUploadFileWorker(unittest.TestCase):
    __test__ = True

    def setUp(self):
        self.tender_id = uuid.uuid4().hex
        self.award_id = uuid.uuid4().hex
        self.qualification_id = uuid.uuid4().hex
        self.document_id = generate_doc_id()
        self.process_tracker = ProcessTracker(db=MagicMock())
        self.process_tracker.set_item(self.tender_id, self.award_id, 1)
        self.upload_to_doc_service_queue = Queue(10)
        self.upload_to_tender_queue = Queue(10)
        self.sleep_change_value = APIRateController()
        self.sna = event.Event()
        self.sna.set()
        self.data = Data(self.tender_id, self.award_id, '123', 'awards',
                         {'meta': {'id': self.document_id}, 'test_data': 'test_data'})
        self.qualification_data = Data(self.tender_id, self.qualification_id, '123', 'qualifications',
                                       {'meta': {'id': self.document_id}, 'test_data': 'test_data'})
        self.doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        self.worker = UploadFileToDocService(self.upload_to_doc_service_queue, self.upload_to_tender_queue,
                                             self.process_tracker, self.doc_service_client, self.sna,
                                             self.sleep_change_value)
        self.url = '{url}'.format(url=self.doc_service_client.url)

    @staticmethod
    def stat_200():
        return {'data': {'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                         'format': 'application/yaml',
                         'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                         'title': file_name}}

    @staticmethod
    def get_tender():
        return {'data': {'id': uuid.uuid4().hex,
                         'documentOf': 'tender',
                         'documentType': DOC_TYPE,
                         'url': 'url'}}

    def tearDown(self):
        del self.worker

    def is_working(self, worker):
        return self.upload_to_doc_service_queue.qsize() or worker.retry_upload_to_doc_service_queue.qsize()

    def shutdown_when_done(self, worker):
        worker.start()
        while self.is_working(worker):
            sleep(0.1)
        worker.shutdown()

    def test_init(self):
        worker = UploadFileToDocService.spawn(None, None, None, None, self.sna, None)
        self.assertGreater(datetime.datetime.now().isoformat(),
                           worker.start_time.isoformat())
        self.assertEqual(worker.upload_to_doc_service_queue, None)
        self.assertEqual(worker.upload_to_tender_queue, None)
        self.assertEqual(worker.process_tracker, None)
        self.assertEqual(worker.doc_service_client, None)
        self.assertEqual(worker.services_not_available, self.sna)
        self.assertEqual(worker.sleep_change_value, None)
        self.assertEqual(worker.delay, 15)
        self.assertEqual(worker.exit, False)
        worker.shutdown()
        self.assertEqual(worker.exit, True)
        del worker

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_successful_upload(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        mrequest.post(self.url, json=self.stat_200(), status_code=200)
        self.upload_to_doc_service_queue.put(self.data)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(), [item_key(self.tender_id, self.award_id)])
        self.assertEqual(self.upload_to_doc_service_queue.qsize(), 1)
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(self.upload_to_tender_queue.qsize(), 1, 'Queue should be have 1 element')
        self.assertEqual(mrequest.call_count, 1)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertItemsEqual(self.process_tracker.processing_items.keys(), [item_key(self.tender_id, self.award_id)])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_doc_service(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.post(self.url, [{'text': '', 'status_code': 401} for _ in range(6)] + [
            {'json': {'data': {'url': 'test url',
                               'format': 'application/yaml',
                               'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                               'title': file_name}},
             'status_code': 200}])
        self.upload_to_doc_service_queue.put(self.data)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(), [item_key(self.tender_id, self.award_id)])
        self.assertEqual(self.upload_to_doc_service_queue.qsize(), 1)
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(self.upload_to_tender_queue.qsize(), 1, 'Queue should be have 1 element')
        self.assertEqual(mrequest.call_count, 7)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_request_failed(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        mrequest.post(self.url, json=self.stat_200(), status_code=200)
        self.upload_to_doc_service_queue.put(self.data)
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(self.upload_to_tender_queue.get(), self.data)
        self.assertEqual(self.process_tracker.processing_items, {item_key(self.tender_id, self.award_id): 1})
        self.assertEqual(mrequest.call_count, 1)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_request_failed_item_status_change(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        mrequest.post(self.url, json=self.stat_200(), status_code=200)
        self.process_tracker.set_item(self.tender_id, self.qualification_id, 1)
        self.upload_to_doc_service_queue.put(self.data)
        self.upload_to_doc_service_queue.put(self.qualification_data)
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(self.upload_to_tender_queue.get(), self.data)
        self.assertEqual(self.upload_to_tender_queue.get(), self.qualification_data)
        self.assertEqual(mrequest.call_count, 2)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertEqual(self.process_tracker.processing_items,
                         {item_key(self.tender_id, self.award_id): 1,
                          item_key(self.tender_id, self.qualification_id): 1})

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_processing_items(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        mrequest.post(self.url, [{'json': self.stat_200(), 'status_code': 200} for _ in range(2)])
        self.process_tracker.set_item(self.tender_id, self.award_id, 2)
        self.upload_to_doc_service_queue.put(self.data)
        self.upload_to_doc_service_queue.put(self.data)
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.upload_to_tender_queue.get(), self.data)
        self.assertEqual(self.upload_to_tender_queue.get(), self.data)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_upload_to_doc_service_queue_loop_exit(self, mrequest, gevent_sleep):
        """ Test LoopExit for upload_to_doc_service_queue """
        gevent_sleep.side_effect = custom_sleep
        self.process_tracker.set_item(self.tender_id, self.award_id, 2)
        self.worker.upload_to_doc_service_queue = MagicMock()
        self.worker.upload_to_doc_service_queue.peek.side_effect = generate_answers(
            answers=[LoopExit(), self.data, self.data], default=LoopExit())
        mrequest.post(self.url, [{'json': self.stat_200(), 'status_code': 200} for _ in range(2)])
        self.worker.start()
        sleep(1)
        self.assertEqual(self.upload_to_tender_queue.get(), self.data)
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(self.process_tracker.processing_items, {item_key(self.tender_id, self.award_id): 2})

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_upload_to_doc_service_queue_loop_exit(self, mrequest, gevent_sleep):
        """ Test LoopExit for retry_upload_to_doc_service_queue """
        gevent_sleep.side_effect = custom_sleep
        mrequest.post(self.url, [{'json': self.stat_200(), 'status_code': 200} for _ in range(2)])
        self.process_tracker.set_item(self.tender_id, self.award_id, 2)
        self.worker.retry_upload_to_doc_service_queue = MagicMock()
        self.worker.retry_upload_to_doc_service_queue.peek.side_effect = generate_answers(
            answers=[LoopExit(), self.data, self.data], default=LoopExit())
        self.worker.start()
        sleep(1)
        self.worker.shutdown()
        self.assertEqual(self.upload_to_tender_queue.get(), self.data)
        self.assertEqual(self.process_tracker.processing_items, {item_key(self.tender_id, self.award_id): 2})
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])

    def test_remove_bad_data(self):
        self.worker.upload_to_doc_service_queue = MagicMock(get=MagicMock())
        self.worker.process_tracker = MagicMock(update_items_and_tender=MagicMock())

        self.worker.remove_bad_data(self.data, Exception("test message"), False)

        self.worker.upload_to_doc_service_queue.get.assert_called_once()
        self.assertEqual(self.worker.retry_upload_to_doc_service_queue.get(), self.data)

    def test_remove_bad_data_retry(self):
        self.worker.retry_upload_to_doc_service_queue = MagicMock(get=MagicMock())
        self.worker.process_tracker = MagicMock(update_items_and_tender=MagicMock())

        with self.assertRaises(Exception):
            self.worker.remove_bad_data(self.data, Exception("test message"), True)

        self.worker.retry_upload_to_doc_service_queue.get.assert_called_once()
        self.worker.process_tracker.update_items_and_tender.assert_called_with(self.data.tender_id, self.data.item_id,
                                                                               self.document_id)

    def test_try_upload_to_doc_service(self):
        e = Exception("test error")
        self.worker.update_headers_and_upload = MagicMock(side_effect=e)
        self.worker.remove_bad_data = MagicMock()

        self.worker.try_upload_to_doc_service(self.data, False)

        self.worker.update_headers_and_upload.assert_called_once()
        self.worker.remove_bad_data.assert_called_once_with(self.data, e, False)

    def test_try_upload_to_doc_service_retry(self):
        e = Exception("test error")
        self.worker.update_headers_and_upload = MagicMock(side_effect=e)
        self.worker.remove_bad_data = MagicMock()

        self.worker.try_upload_to_doc_service(self.data, True)

        self.worker.update_headers_and_upload.assert_called_once()
        self.worker.remove_bad_data.assert_called_with(self.data, e, True)

    def test_run(self):
        self.worker.delay = 1
        upload_worker, retry_upload_worker = MagicMock(), MagicMock()
        self.worker.upload_worker = upload_worker
        self.worker.retry_upload_worker = retry_upload_worker
        with patch.object(self.worker, 'exit', AlmostAlwaysFalse()):
            self.worker._run()
        self.assertEqual(self.worker.upload_worker.call_count, 1)
        self.assertEqual(self.worker.retry_upload_worker.call_count, 1)

    @patch('gevent.killall')
    def test_run_exception(self, killlall):
        self.worker.delay = 1
        self.worker._start_jobs = MagicMock(return_value={"a": 1})
        self.worker.check_and_revive_jobs = MagicMock(side_effect=Exception("test error"))
        self.worker._run()
        killlall.assert_called_once_with([1], timeout=5)

    @patch('gevent.killall')
    @patch('gevent.sleep')
    def test_run_exception(self, gevent_sleep, killlall):
        gevent_sleep.side_effect = custom_sleep
        self.worker._start_jobs = MagicMock(return_value={"a": 1})
        self.worker.check_and_revive_jobs = MagicMock(side_effect=Exception("test error"))

        self.worker._run()

        killlall.assert_called_once_with([1], timeout=5)
