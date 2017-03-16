# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import uuid
import unittest
import datetime
import requests_mock
import requests

from time import sleep
from gevent.queue import Queue
from mock import patch, MagicMock

from openprocurement.bot.identification.databridge.edr_handler import EdrHandler
from openprocurement.bot.identification.databridge.utils import Data
from openprocurement.bot.identification.tests.utils import custom_sleep
from openprocurement.bot.identification.client import ProxyClient


class TestEdrHandlerWorker(unittest.TestCase):

    def test_init(self):
        worker = EdrHandler.spawn(None, None, None, None)
        self.assertGreater(datetime.datetime.now().isoformat(),
                           worker.start_time.isoformat())

        self.assertEqual(worker.proxyClient, None)
        self.assertEqual(worker.edrpou_codes_queue, None)
        self.assertEqual(worker.edr_ids_queue, None)
        self.assertEqual(worker.upload_to_doc_service_queue, None)
        self.assertEqual(worker.delay, 15)
        self.assertEqual(worker.exit, False)

        worker.shutdown()
        self.assertEqual(worker.exit, True)
        del worker

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_proxy_client(self, mrequest, gevent_sleep):
        """ Test that proxy return json with id """
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', token='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'json': {'data': [{'id': '321'}]}, 'status_code': 200},
                      {'json': {'data': [{'id': '321'}]}, 'status_code': 200}])

        edrpou_codes_queue = Queue(10)
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '123', "awards", None, None))
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '135', "awards", None, None))

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, MagicMock(), MagicMock())

        sleep(10)

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')
        self.assertEqual(mrequest.call_count, 2)
        self.assertEqual(mrequest.request_history[0].url,
                         u'127.0.0.1:80/verify?code=123')
        self.assertEqual(mrequest.request_history[1].url,
                         u'127.0.0.1:80/verify?code=135')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_proxy_client_401(self, mrequest, gevent_sleep):
        """ After 401 need restart worker """
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', token='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'text': '', 'status_code': 401},
                      {'json': {'data': [{'id': '321'}]}, 'status_code': 200},
                      {'json': {'data': [{'id': '321'}]}, 'status_code': 200}])

        edrpou_codes_queue = Queue(10)
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '123', "awards", None, None))
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '135', "awards", None, None))

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue,
                                  MagicMock(), MagicMock())

        sleep(3)
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')
        self.assertEqual(mrequest.call_count, 3)  # Requests must call proxy three times
        self.assertEqual(mrequest.request_history[0].url,
                         u'127.0.0.1:80/verify?code=123')  # First return 401
        self.assertEqual(mrequest.request_history[1].url,
                         u'127.0.0.1:80/verify?code=123')  # From retry
        self.assertEqual(mrequest.request_history[2].url,
                         u'127.0.0.1:80/verify?code=135')  # Resume normal work


    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_proxy_client_429(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', token='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'text': '', 'status_code': 429, 'headers': {'Retry-After': '10'}},
                      {'json': {'data': [{'id': '321'}]}, 'status_code': 200},
                      {'json': {'data': [{'id': '321'}]}, 'status_code': 200}])

        edrpou_codes_queue = Queue(10)
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '123', "awards", None, None))
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '135', "awards", None, None))

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, MagicMock(), MagicMock())

        sleep(5)
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')
        self.assertEqual(mrequest.call_count, 3)  # Requests must call proxy three times
        self.assertEqual(mrequest.request_history[0].url,
                         u'127.0.0.1:80/verify?code=123')  # First return 429
        self.assertEqual(mrequest.request_history[1].url,
                         u'127.0.0.1:80/verify?code=123')
        self.assertEqual(mrequest.request_history[2].url,
                         u'127.0.0.1:80/verify?code=135')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_proxy_client_402(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', token='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'text': '', 'status_code': 402},  # pay for me
                      {'json': {'data': [{'id': '321'}]}, 'status_code': 200},
                      {'json': {'data': [{'id': '321'}]}, 'status_code': 200}])

        edrpou_codes_queue = Queue(10)
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '123', "awards", None, None))
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '135', "awards", None, None))

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue,
                                  MagicMock(), MagicMock())

        sleep(5)
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')
        self.assertEqual(mrequest.call_count, 3)  # Requests must call proxy three times
        self.assertEqual(mrequest.request_history[0].url,
                         u'127.0.0.1:80/verify?code=123')  # First return 402
        self.assertEqual(mrequest.request_history[1].url,
                         u'127.0.0.1:80/verify?code=123')
        self.assertEqual(mrequest.request_history[2].url,
                         u'127.0.0.1:80/verify?code=135')


    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_not_found_edrpou(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', token='')
        mrequest.get("{url}".format(url=proxy_client.verify_url), json={'data': []}, status_code=200)

        edrpou_codes_queue = Queue(10)
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '123', "awards", None, None))

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, MagicMock(), MagicMock())

        sleep(1)
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')
        self.assertEqual(mrequest.call_count, 1)  # Requests must call proxy three times
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/verify?code=123')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_id(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', token='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'text': '', 'status_code': 402},
                      {'text': '', 'status_code': 402},
                      {'json': {'data': [{'id': '321'}]}, 'status_code': 200}])

        edrpou_codes_queue = Queue(10)
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '123', "awards", None, None))

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue,
                                  MagicMock(), MagicMock())

        sleep(5)
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')
        self.assertEqual(mrequest.call_count, 3)  # Requests must call proxy three times
        self.assertEqual(mrequest.request_history[0].url,
                         u'127.0.0.1:80/verify?code=123')  # First return 402
        self.assertEqual(mrequest.request_history[1].url,
                         u'127.0.0.1:80/verify?code=123')
        self.assertEqual(mrequest.request_history[2].url,
                         u'127.0.0.1:80/verify?code=123')


    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_id_empty_response(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', token='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'text': '', 'status_code': 402},
                      {'text': '', 'status_code': 402},
                      {'json': {'data': []}, 'status_code': 200}])

        edrpou_codes_queue = Queue(10)
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '123', "awards", None, None))

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue,
                                  MagicMock(), MagicMock())

        sleep(5)
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')
        self.assertEqual(mrequest.call_count, 3)  # Requests must call proxy three times
        self.assertEqual(mrequest.request_history[0].url,
                         u'127.0.0.1:80/verify?code=123')  # First return 402
        self.assertEqual(mrequest.request_history[1].url,
                         u'127.0.0.1:80/verify?code=123')
        self.assertEqual(mrequest.request_history[2].url,
                         u'127.0.0.1:80/verify?code=123')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_successful_get_edr_details(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', token='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     json={'data': [{'id': '321'}, {'id': '322'}]}, status_code=200)
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), json={'data': {}}, status_code=200)
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=322), json={'data': {}}, status_code=200)
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)
        sleep(3)
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(upload_to_doc_service_queue.qsize(), 2)
        self.assertEqual(mrequest.call_count, 3)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/verify?code=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/details/321')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/details/322')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_details(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', token='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     json={'data': [{'id': '321'}, {'id': '322'}]}, status_code=200)
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), json={'data': {}}, status_code=200)
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=322),
                     [{'text': '', 'status_code': 402}, {'json': {'data': {}}, 'status_code': 200}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)
        sleep(3)
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(upload_to_doc_service_queue.qsize(), 2)
        self.assertEqual(mrequest.call_count, 4)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/verify?code=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/details/321')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/details/322')
        self.assertEqual(mrequest.request_history[3].url, u'127.0.0.1:80/details/322')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_job_get_edr_id_dead(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', token='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     [{'json': {'data': {'id': '321'}}, 'status_code': 200},
                      {'json': {'data': [{'id': '321'}]}, 'status_code': 200}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), json={'data': {}}, status_code=200)
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)
        sleep(3)
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        self.assertEqual(mrequest.call_count, 3)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/verify?code=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/verify?code=123')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/details/321')


    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_job_get_edr_details_dead(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', token='')
        mrequest.get("{url}".format(url=proxy_client.verify_url), json={'data': [{'id': '321'}]}, status_code=200)
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321),
                     [{'json': [], 'status_code': 200},
                      {'json': {'data': {}}, 'status_code': 200}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)
        sleep(3)
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        self.assertEqual(mrequest.call_count, 3)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/verify?code=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/details/321')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/details/321')


    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_exception(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', token='')
        mrequest.get("{url}".format(url=proxy_client.verify_url), json={'data': [{'id': '321'}]}, status_code=200)
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321),
                     [{'text': '', 'status_code': 403},
                      {'text': '', 'status_code': 402},
                      {'text': '', 'status_code': 402},
                      {'text': '', 'status_code': 402},
                      {'text': '', 'status_code': 402},
                      {'text': '', 'status_code': 402},
                      {'json': {'data': {}}, 'status_code': 200}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)
        sleep(60)
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        self.assertEqual(mrequest.call_count, 8)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/verify?code=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/details/321')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/details/321')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_timeout(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', token='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     [{'exc': requests.exceptions.ReadTimeout}, {'json': {'data': [{'id': 321}]}, 'status_code': 200}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321),
                     [{'exc': requests.exceptions.ReadTimeout}, {'json': {'data': {'id': 321}}, 'status_code': 200}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)
        sleep(5)
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        self.assertEqual(mrequest.call_count, 4)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/verify?code=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/verify?code=123')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/details/321')
        self.assertEqual(mrequest.request_history[3].url, u'127.0.0.1:80/details/321')


