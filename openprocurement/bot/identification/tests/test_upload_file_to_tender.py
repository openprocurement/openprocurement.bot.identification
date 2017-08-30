# coding=utf-8
from gevent import monkey

monkey.patch_all()

import uuid
import unittest

from gevent.queue import Queue
from gevent.hub import LoopExit
from time import sleep
from mock import patch, MagicMock
from restkit.errors import Unauthorized
from restkit import ResourceError
from gevent.pywsgi import WSGIServer
from bottle import Bottle, response
from simplejson import dumps

from openprocurement.bot.identification.databridge.upload_file_to_tender import UploadFileToTender
from openprocurement.bot.identification.databridge.utils import generate_doc_id, item_key
from openprocurement.bot.identification.databridge.process_tracker import ProcessTracker
from openprocurement.bot.identification.databridge.data import Data
from openprocurement.bot.identification.tests.utils import custom_sleep, generate_answers, AlmostAlwaysFalse
from openprocurement.bot.identification.databridge.constants import file_name, DOC_TYPE
from openprocurement.bot.identification.databridge.bridge import TendersClientSync, TendersClient
from openprocurement.bot.identification.databridge.sleep_change_value import APIRateController

SERVER_RESPONSE_FLAG = 0
SERVER_RETRY_COUNTER = 2
SPORE_COOKIES = ("a7afc9b1fc79e640f2487ba48243ca071c07a823d27"
                 "8cf9b7adf0fae467a524747e3c6c6973262130fac2b"
                 "96a11693fa8bd38623e4daee121f60b4301aef012c")
COOKIES_412 = ("b7afc9b1fc79e640f2487ba48243ca071c07a823d27"
               "8cf9b7adf0fae467a524747e3c6c6973262130fac2b"
               "96a11693fa8bd38623e4daee121f60b4301aef012c")


def setup_routing(app, func, path='/api/2.3/spore', method='GET'):
    app.routes = []
    app.route(path, method, func)


def response_spore():
    response.set_cookie("SERVER_ID", SPORE_COOKIES)
    return response


def response_412():
    response.status = 412
    response.set_cookie("SERVER_ID", COOKIES_412)
    return response


def response_get_tender():
    response.status = 201
    response.headers['X-Request-ID'] = '125'
    return dumps({'data': {'id': uuid.uuid4().hex,
                           'documentOf': 'tender',
                           'documentType': DOC_TYPE,
                           'url': 'url'}})


def generate_response():
    global SERVER_RESPONSE_FLAG
    if SERVER_RESPONSE_FLAG == 0:
        SERVER_RESPONSE_FLAG = 1
        return response_412()
    return response_get_tender()


def generate_response_retry():
    global SERVER_RETRY_COUNTER
    if SERVER_RETRY_COUNTER > 0:
        SERVER_RETRY_COUNTER -= 1
        return response_412()
    return response_get_tender()


class TestUploadFileToTenderWorker(unittest.TestCase):
    __test__ = True

    def setUp(self):
        self.tender_id = uuid.uuid4().hex
        self.award_id = uuid.uuid4().hex
        self.qualification_id = uuid.uuid4().hex
        self.document_id = generate_doc_id()
        self.process_tracker = ProcessTracker(db=MagicMock())
        self.process_tracker.set_item(self.tender_id, self.award_id, 1)
        self.upload_to_tender_queue = Queue(10)
        self.url = 'http://127.0.0.1:20604'
        self.sleep_change_value = APIRateController()
        self.data = Data(self.tender_id, self.award_id, '123', 'awards',
                         {'meta': {'id': self.document_id}, 'test_data': 'test_data'})
        self.qualification_data = Data(self.tender_id, self.qualification_id, '123', 'qualifications',
                                       {'meta': {'id': self.document_id}, 'test_data': 'test_data'})
        self.client = MagicMock()
        self.worker = UploadFileToTender(self.client, self.upload_to_tender_queue,
                                         self.process_tracker, MagicMock(), self.sleep_change_value)
        self.worker.retry_upload_to_tender_queue = Queue(10)

    def tearDown(self):
        del self.worker
        del self.upload_to_tender_queue

    @staticmethod
    def get_tender():
        return {'data': {'id': uuid.uuid4().hex,
                         'documentOf': 'tender',
                         'documentType': DOC_TYPE,
                         'url': 'url'}}

    def is_working(self, worker):
        return self.upload_to_tender_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()

    def shutdown_when_done(self, worker):
        worker.start()
        while self.is_working(worker):
            sleep(0.1)
        worker.shutdown()

    @patch('gevent.sleep')
    def test_upload_to_tender_429(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.client._create_tender_resource_item = MagicMock(side_effect=[
            ResourceError(http_code=429), ResourceError(http_code=429), ResourceError(http_code=403)])
        self.upload_to_tender_queue.put(self.data)
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(self.worker.sleep_change_value.time_between_requests, 1)

    @patch('gevent.sleep')
    def test_upload_to_tender_exception(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.upload_to_tender_queue.put(self.data)
        self.client._create_tender_resource_item = MagicMock(side_effect=[Exception()])
        self.worker.do_upload_to_tender_with_retry = MagicMock(side_effect=ResourceError(http_code=403))
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(self.worker.sleep_change_value.time_between_requests, 0)

    @patch('gevent.sleep')
    def test_upload_to_tender_exception_status_int_none(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.upload_to_tender_queue.put(self.data)
        client = MagicMock()
        client._create_tender_resource_item = MagicMock(side_effect=[Unauthorized()])
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(self.worker.sleep_change_value.time_between_requests, 0)

    @patch('gevent.sleep')
    def test_retry_upload_to_tender(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.client._create_tender_resource_item.side_effect = [Unauthorized(http_code=401),
                                                                Unauthorized(http_code=403),
                                                                Unauthorized(http_code=429), self.get_tender()]
        self.upload_to_tender_queue.put(self.data)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(), [item_key(self.tender_id, self.award_id)])
        self.assertEqual(self.upload_to_tender_queue.qsize(), 1)
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(self.process_tracker.processing_items, {})  # test that item removed from processing_items
        self.assertEqual(self.client._create_tender_resource_item.call_count, 4)  # check upload to tender

    @patch('gevent.sleep')
    def test_retry_upload_to_tender_422(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.client_upload_to_tender = MagicMock(side_effect=ResourceError(http_code=422))
        self.worker.retry_upload_to_tender_queue = Queue(10)
        self.worker.retry_upload_to_tender_queue.put(self.data)
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.upload_to_tender_queue.qsize(), 0, 'Queue should be empty')

    @patch('gevent.sleep')
    def test_retry_upload_to_tender_429(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.client.client_upload_to_tender = MagicMock(
            side_effect=[ResourceError(http_code=429), ResourceError(http_code=403)])
        self.worker.retry_upload_to_tender_queue = Queue(10)
        self.worker.retry_upload_to_tender_queue.put(self.data)
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.upload_to_tender_queue.qsize(), 0, 'Queue should be empty')

    @patch('gevent.sleep')
    def test_retry_upload_to_tender_exception(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.worker.do_upload_to_tender_with_retry = MagicMock(side_effect=[Exception(), ResourceError(http_code=403)])
        self.worker.retry_upload_to_tender_queue.put(self.data)
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.upload_to_tender_queue.qsize(), 0, 'Queue should be empty')

    @patch('gevent.sleep')
    def test_upload_to_tender_queue_loop_exit(self, gevent_sleep):
        """ Test LoopExit for upload_to_tender_queue """
        gevent_sleep.side_effect = custom_sleep
        self.client._create_tender_resource_item.side_effect = [self.get_tender() for _ in range(2)]
        self.process_tracker.set_item(self.tender_id, self.award_id, 2)
        self.worker.upload_to_tender_queue = MagicMock()
        self.worker.upload_to_tender_queue.peek.side_effect = generate_answers(
            answers=[LoopExit(), self.datum(), self.datum()], default=LoopExit())
        self.worker.start()
        sleep(1)
        self.worker.shutdown()
        self.assertEqual(self.process_tracker.processing_items, {})
        self.assertIsNotNone(self.client.request_history[0].headers['X-Client-Request-ID'])
        self.assertIsNotNone(self.client.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(self.client._create_tender_resource_item.call_count, 2)  # check that processed just 1 request

    def datum(self):
        return Data(self.tender_id, self.award_id, '123', 'awards',
                    {u'meta': {u'id': self.document_id},
                     u'url': u'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                     u'format': u'application/yaml',
                     u'hash': u'md5:9a0364b9e99bb480dd25e1f0284c8555',
                     u'title': file_name})

    @patch('gevent.sleep')
    def test_retry_upload_to_tender_queue_loop_exit(self, gevent_sleep):
        """ Test LoopExit for retry_upload_to_tender_queue """
        gevent_sleep.side_effect = custom_sleep
        self.client._create_tender_resource_item.side_effect = [self.get_tender() for _ in range(2)]
        self.worker.retry_upload_to_tender_queue = MagicMock()
        self.worker.retry_upload_to_tender_queue.peek.side_effect = generate_answers(
            answers=[LoopExit(), self.datum(), self.datum()], default=LoopExit())
        self.process_tracker.set_item(self.tender_id, self.award_id, 2)
        self.worker.start()
        sleep(1)
        self.worker.shutdown()
        self.assertEqual(self.process_tracker.processing_items, {})
        self.assertIsNotNone(self.client.request_history[0].headers['X-Client-Request-ID'])
        self.assertIsNotNone(self.client.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(self.client._create_tender_resource_item.call_count, 2)  # check that processed just 1 request

    @patch('gevent.sleep')
    def test_request_failed_in_retry_item_status(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.client._create_tender_resource_item.side_effect = [ResourceError(http_code=429)] + [
            ResourceError(http_code=403) for _ in range(4)]
        self.worker.retry_upload_to_tender_queue.put(self.data)
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.upload_to_tender_queue.qsize(), 0, 'Queue should be empty')

    @patch('gevent.sleep')
    def test_request_failed_in_retry(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.worker.do_upload_to_tender_with_retry = MagicMock()
        self.worker.do_upload_to_tender_with_retry.side_effect = [ResourceError(http_code=429) for _ in range(5)] + [
            ResourceError(http_code=403)]
        self.sleep_change_value.increment_step = 3
        self.sleep_change_value.decrement_step = 1.5
        self.worker.retry_upload_to_tender_queue.put(self.data)
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(self.worker.sleep_change_value.time_between_requests, 13.5)

    @patch('gevent.sleep')
    def test_process_412(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        api_server_bottle = Bottle()
        api_server = WSGIServer(('127.0.0.1', 20604), api_server_bottle, log=None)
        setup_routing(api_server_bottle, response_spore)
        setup_routing(api_server_bottle, response_412, path='/api/2.3/tenders/{}/awards/{}/documents'.format(
            self.tender_id, self.award_id), method='POST')
        api_server.start()
        self.worker.client = TendersClient('', host_url='http://127.0.0.1:20604', api_version='2.3')
        setup_routing(api_server_bottle, generate_response, path='/api/2.3/tenders/{}/awards/{}/documents'.format(
            self.tender_id, self.award_id), method='POST')
        self.assertEqual(self.worker.client.headers['Cookie'], 'SERVER_ID={}'.format(SPORE_COOKIES))
        self.upload_to_tender_queue.put(self.data)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(), [item_key(self.tender_id, self.award_id)])
        self.assertEqual(self.upload_to_tender_queue.qsize(), 1)
        self.shutdown_when_done(self.worker)
        self.assertEqual(self.worker.client.headers['Cookie'], 'SERVER_ID={}'.format(COOKIES_412))
        self.assertEqual(self.upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(self.worker.retry_upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertItemsEqual(self.process_tracker.processing_items.keys(), [])
        api_server.stop()

    @patch('gevent.sleep')
    def test_upload_worker(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.worker.services_not_available = MagicMock(wait=MagicMock())
        self.worker.try_peek_data_and_upload_to_tender = MagicMock()
        with patch.object(self.worker, 'exit', AlmostAlwaysFalse()):
            self.worker.upload_worker()
        self.worker.services_not_available.wait.assert_called_once()
        self.worker.try_peek_data_and_upload_to_tender.assert_called_once_with(False)

    @patch('gevent.sleep')
    def test_retry_upload_worker(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.worker.services_not_available = MagicMock(wait=MagicMock())
        self.worker.try_peek_data_and_upload_to_tender = MagicMock()
        with patch.object(self.worker, 'exit', AlmostAlwaysFalse()):
            self.worker.retry_upload_worker()
        self.worker.services_not_available.wait.assert_called_once()
        self.worker.try_peek_data_and_upload_to_tender.assert_called_once_with(True)

    def test_peek_from_tender_queue(self):
        self.worker.upload_to_tender_queue.put(self.data)
        self.assertEqual(self.worker.peek_from_tender_queue(False), self.data)

    def test_peek_from_tender_queue_retry(self):
        self.worker.retry_upload_to_tender_queue.put(self.data)
        self.assertEqual(self.worker.peek_from_tender_queue(True), self.data)

    def test_peek_from_tender_queue_empty(self):
        self.worker.upload_to_tender_queue = MagicMock(peek=MagicMock(side_effect=LoopExit))
        with self.assertRaises(LoopExit):
            self.worker.peek_from_tender_queue(False)

    def test_peek_from_tender_queue_retry_empty(self):
        self.worker.retry_upload_to_tender_queue = MagicMock(peek=MagicMock(side_effect=LoopExit))
        with self.assertRaises(LoopExit):
            self.worker.peek_from_tender_queue(True)

    def test_try_peek_data_and_upload_to_tender(self):
        self.worker.upload_to_tender_queue.put(self.data)
        self.worker.try_upload_to_tender = MagicMock()
        self.worker.try_peek_data_and_upload_to_tender(False)
        self.worker.try_upload_to_tender.assert_called_once_with(self.data, False)

    def test_try_peek_data_and_upload_to_tender_retry(self):
        self.worker.retry_upload_to_tender_queue.put(self.data)
        self.worker.try_upload_to_tender = MagicMock()
        self.worker.try_peek_data_and_upload_to_tender(True)
        self.worker.try_upload_to_tender.assert_called_once_with(self.data, True)

    def test_try_upload_to_tender(self):
        self.worker.update_headers_and_upload_to_tender = MagicMock()
        self.worker.successfully_uploaded_to_tender = MagicMock()
        self.worker.try_upload_to_tender(self.data, False)
        self.worker.update_headers_and_upload_to_tender.assert_called_once_with(self.data, False)
        self.worker.successfully_uploaded_to_tender.assert_called_once_with(self.data, False)

    def test_try_upload_to_tender_retry(self):
        self.worker.update_headers_and_upload_to_tender = MagicMock()
        self.worker.successfully_uploaded_to_tender = MagicMock()
        self.worker.try_upload_to_tender(self.data, True)
        self.worker.update_headers_and_upload_to_tender.assert_called_once_with(self.data, True)
        self.worker.successfully_uploaded_to_tender.assert_called_once_with(self.data, True)

    def test_try_upload_to_tender_no_mock(self):
        self.upload_to_tender_queue.put(self.data)
        self.worker.try_upload_to_tender(self.data, False)
        self.assertEqual(self.upload_to_tender_queue.qsize(), 0)
        self.assertEqual(self.process_tracker.processing_items, {})

    def test_try_upload_to_tender_no_mock_retry(self):
        self.worker.retry_upload_to_tender_queue.put(self.data)
        self.worker.try_upload_to_tender(self.data, True)
        self.assertEqual(self.upload_to_tender_queue.qsize(), 0)
        self.assertEqual(self.process_tracker.processing_items, {})

    def test_try_upload_to_tender_resource_error(self):
        re = ResourceError("test resource error")
        self.worker.update_headers_and_upload_to_tender = MagicMock(side_effect=re)
        self.worker.remove_data_or_increase_wait = MagicMock()
        self.worker.try_upload_to_tender(self.data, False)
        self.worker.remove_data_or_increase_wait.assert_called_once_with(re, self.data, False)

    def test_try_upload_to_tender_exception(self):
        e = Exception("exception")
        self.worker.update_headers_and_upload_to_tender = MagicMock(side_effect=e)
        self.worker.handle_error = MagicMock()
        self.worker.try_upload_to_tender(self.data, False)
        self.worker.handle_error.assert_called_once_with(e, self.data, False)

    def test_update_headers_and_upload_to_tender(self):
        self.worker.do_upload_to_tender = MagicMock()
        self.worker.update_headers_and_upload_to_tender(self.data, False)
        self.worker.do_upload_to_tender.assert_called_once_with(self.data)

    def test_update_headers_and_upload_to_tender_retry(self):
        self.worker.do_upload_to_tender_with_retry = MagicMock()
        self.worker.update_headers_and_upload_to_tender(self.data, True)
        self.worker.do_upload_to_tender_with_retry.assert_called_once_with(self.data)

    def test_do_upload_to_tender(self):
        api_server_bottle = Bottle()
        api_server = WSGIServer(('127.0.0.1', 20604), api_server_bottle, log=None)
        setup_routing(api_server_bottle, response_spore)
        api_server.start()
        self.worker.client = TendersClientSync('', host_url='http://127.0.0.1:20604', api_version='2.3')
        setup_routing(api_server_bottle, response_get_tender, path='/api/2.3/tenders/{}/awards/{}/documents'.format(
            self.tender_id, self.award_id), method='POST')
        self.worker.do_upload_to_tender(self.data)
        api_server.stop()

    def test_do_upload_to_tender_failure(self):
        api_server_bottle = Bottle()
        api_server = WSGIServer(('127.0.0.1', 20604), api_server_bottle, log=None)
        setup_routing(api_server_bottle, response_spore)
        api_server.start()
        self.worker.client = TendersClientSync('', host_url='http://127.0.0.1:20604', api_version='2.3')
        setup_routing(api_server_bottle, response_412, path='/api/2.3/tenders/{}/awards/{}/documents'.format(
            self.tender_id, self.award_id), method='POST')
        with self.assertRaises(ResourceError):
            self.worker.do_upload_to_tender(self.data)
        api_server.stop()

    def test_do_upload_to_tender_with_retry(self):
        api_server_bottle = Bottle()
        api_server = WSGIServer(('127.0.0.1', 20604), api_server_bottle, log=None)
        setup_routing(api_server_bottle, response_spore)
        api_server.start()
        self.worker.client = TendersClientSync('', host_url='http://127.0.0.1:20604', api_version='2.3')
        setup_routing(api_server_bottle, response_get_tender, path='/api/2.3/tenders/{}/awards/{}/documents'.format(
            self.tender_id, self.award_id), method='POST')
        self.worker.do_upload_to_tender_with_retry(self.data)
        api_server.stop()

    def test_do_upload_to_tender_with_retry_fail_then_success(self):
        api_server_bottle = Bottle()
        api_server = WSGIServer(('127.0.0.1', 20604), api_server_bottle, log=None)
        setup_routing(api_server_bottle, response_spore)
        api_server.start()
        self.worker.client = TendersClientSync('', host_url='http://127.0.0.1:20604', api_version='2.3')
        setup_routing(api_server_bottle, generate_response_retry, path='/api/2.3/tenders/{}/awards/{}/documents'.format(
            self.tender_id, self.award_id), method='POST')
        self.worker.do_upload_to_tender_with_retry(self.data)
        api_server.stop()

    def test_do_upload_to_tender_with_retry_fail(self):
        api_server_bottle = Bottle()
        api_server = WSGIServer(('127.0.0.1', 20604), api_server_bottle, log=None)
        setup_routing(api_server_bottle, response_spore)
        api_server.start()
        self.worker.client = TendersClientSync('', host_url='http://127.0.0.1:20604', api_version='2.3')
        setup_routing(api_server_bottle, response_412, path='/api/2.3/tenders/{}/awards/{}/documents'.format(
            self.tender_id, self.award_id), method='POST')
        with self.assertRaises(ResourceError):
            self.worker.do_upload_to_tender_with_retry(self.data)
        api_server.stop()

    def test_remove_data_or_increase_wait(self):
        re = ResourceError("error")
        self.worker.removing_data = MagicMock()
        self.worker.remove_data_or_increase_wait(re, self.data, False)
        self.worker.removing_data.assert_called_once_with(re, self.data, False)

    def test_remove_data_or_increase_wait_429(self):
        re = ResourceError("error", http_code=429)
        self.worker.decrease_request_frequency = MagicMock()
        self.worker.remove_data_or_increase_wait(re, self.data, False)
        self.worker.decrease_request_frequency.assert_called_once_with(re, self.data)

    def test_remove_data_or_increase_wait_else(self):
        re = ResourceError("error", http_code=404)
        self.worker.handle_error = MagicMock()
        self.worker.remove_data_or_increase_wait(re, self.data, False)
        self.worker.handle_error.assert_called_once_with(re, self.data, False)

    def test_removing_data(self):
        re = ResourceError("error")
        self.worker.sleep_change_value.time_between_requests = 1
        self.worker.upload_to_tender_queue.put(self.data)
        self.worker.removing_data(re, self.data, False)
        self.assertEqual(self.worker.process_tracker.processing_items, {})
        self.assertEqual(self.worker.upload_to_tender_queue.qsize(), 0)
        self.assertEqual(self.worker.sleep_change_value.time_between_requests, 0)

    def test_removing_data_retry(self):
        re = ResourceError("error")
        self.worker.sleep_change_value.time_between_requests = 1
        self.worker.retry_upload_to_tender_queue.put(self.data)
        self.worker.removing_data(re, self.data, True)
        self.assertEqual(self.worker.process_tracker.processing_items, {})
        self.assertEqual(self.worker.upload_to_tender_queue.qsize(), 0)
        self.assertEqual(self.worker.retry_upload_to_tender_queue.qsize(), 0)
        self.assertEqual(self.worker.sleep_change_value.time_between_requests, 0)

    def test_decrease_request_frequency(self):
        re = ResourceError("error", 429)
        self.worker.decrease_request_frequency(re, self.data)
        self.assertEqual(self.worker.sleep_change_value.time_between_requests, 1)

    def test_handle_error(self):
        re = ResourceError("error", 404)
        self.worker.upload_to_tender_queue.put(self.data)
        self.worker.handle_error(re, self.data, False)
        self.assertEqual(self.worker.upload_to_tender_queue.qsize(), 0)
        self.assertEqual(self.worker.retry_upload_to_tender_queue.get(), self.data)
        self.assertEqual(self.worker.retry_upload_to_tender_queue.qsize(), 0)

    def test_handle_error_retry(self):
        re = ResourceError("error", 404)
        self.worker.upload_to_tender_queue.put(self.data)
        self.worker.handle_error(re, self.data, True)
        self.assertEqual(self.worker.upload_to_tender_queue.qsize(), 1)
        self.assertEqual(self.worker.retry_upload_to_tender_queue.qsize(), 0)

    def test_successfully_uploaded_to_tender(self):
        self.worker.upload_to_tender_queue.put(self.data)
        self.assertEqual(self.worker.process_tracker.processing_items, {item_key(self.tender_id, self.award_id): 1})
        self.worker.successfully_uploaded_to_tender(self.data, False)
        self.assertEqual(self.worker.upload_to_tender_queue.qsize(), 0)
        self.assertEqual(self.worker.process_tracker.processing_items, {})

    def test_successfully_uploaded_to_tender_retry(self):
        self.worker.retry_upload_to_tender_queue.put(self.data)
        self.assertEqual(self.worker.process_tracker.processing_items, {item_key(self.tender_id, self.award_id): 1})
        self.worker.successfully_uploaded_to_tender(self.data, True)
        self.assertEqual(self.worker.retry_upload_to_tender_queue.qsize(), 0)
        self.assertEqual(self.worker.process_tracker.processing_items, {})

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
    @patch('gevent.sleep')
    def test_run_exception(self, gevent_sleep, killlall):
        gevent_sleep.side_effect = custom_sleep
        self.worker._start_jobs = MagicMock(return_value={"a": 1})
        self.worker.check_and_revive_jobs = MagicMock(side_effect=Exception("test error"))
        self.worker._run()
        killlall.assert_called_once_with([1], timeout=5)

    @patch('gevent.killall')
    def test_run_exception(self, killlall):
        self.worker.delay = 1
        self.worker._start_jobs = MagicMock(return_value={"a": 1})
        self.worker.check_and_revive_jobs = MagicMock(side_effect=Exception("test error"))
        self.worker._run()
        killlall.assert_called_once_with([1], timeout=5)
