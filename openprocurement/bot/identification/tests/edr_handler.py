# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()

import uuid
import unittest
import datetime
import requests_mock
import random

from gevent.queue import Queue, Empty
from gevent.hub import LoopExit
from mock import patch, MagicMock
from munch import munchify

from openprocurement.bot.identification.databridge.edr_handler import EdrHandler
from openprocurement.bot.identification.databridge.filter_tender import FilterTenders
from openprocurement.bot.identification.databridge.utils import Data, generate_doc_id, RetryException
from openprocurement.bot.identification.tests.utils import custom_sleep, generate_answers, generate_request_id, ResponseMock
from openprocurement.bot.identification.client import ProxyClient
from openprocurement.bot.identification.databridge.constants import version, author


def get_random_edr_ids(count=1):
    return [str(random.randrange(10000000, 99999999)) for _ in range(count)]


class TestEdrHandlerWorker(unittest.TestCase):

    def test_init(self):
        worker = EdrHandler.spawn(None, None, None, None, None)
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
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        local_edr_ids = get_random_edr_ids(2)
        edr_req_ids = [generate_request_id(), generate_request_id()]
        edr_details_req_ids = [generate_request_id(), generate_request_id()]
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'json': {'data': [{'x_edrInternalId': local_edr_ids[0]}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_ids[0]}},
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[1]}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200,  'headers': {'X-Request-ID': edr_req_ids[1]}}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=local_edr_ids[0]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_ids[0]})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=local_edr_ids[1]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_ids[1]})

        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        check_queue = Queue(10)

        expected_result = []
        for i in range(2):
            tender_id = uuid.uuid4().hex
            award_id = uuid.uuid4().hex
            document_id = generate_doc_id()
            edr_ids = [str(random.randrange(10000000, 99999999)) for _ in range(2)]
            edrpou_codes_queue.put(Data(tender_id, award_id, edr_ids[i], "awards", None,
                                        {'meta': {'id': document_id, 'author': author,
                                                  'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))  # data
            expected_result.append(Data(tender_id, award_id, edr_ids[i], "awards", [local_edr_ids[i]],
                                        {"data": {}, 'meta': {'sourceDate': '2017-04-25T11:56:36+00:00',
                                                              'id': document_id, "version": version, 'author': author,
                                                              'sourceRequests': [
                                                                  'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                  edr_req_ids[i], edr_details_req_ids[i]]}}))  # result

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, check_queue, MagicMock())

        for result in expected_result:
            self.assertEquals(check_queue.get(), result)

        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id={edr_code}'.format(edr_code=expected_result[0].code))
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/details/{edr_code}'.format(edr_code=expected_result[0].edr_ids[0]))
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/verify?id={edr_code}'.format(edr_code=expected_result[1].code))
        self.assertIsNotNone(mrequest.request_history[3].headers['X-Client-Request-ID'])

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')
        self.assertEqual(mrequest.call_count, 4)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_proxy_client_429(self, mrequest, gevent_sleep):
        """Accept 429 status code in first request with header 'Retry-After'"""
        local_edr_ids = get_random_edr_ids(2)
        gevent_sleep.side_effect = custom_sleep
        edr_req_ids = [generate_request_id(), generate_request_id()]
        edr_details_req_ids = [generate_request_id(), generate_request_id()]
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'json': {'errors': [{'description': ''}]}, 'status_code': 429, 'headers': {'Retry-After': '1', 'X-Request-ID': edr_req_ids[0]}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 429, 'headers': {'Retry-After': '1', 'X-Request-ID': edr_req_ids[0]}},
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[0]}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}},
                       'status_code': 200, 'headers': {'X-Request-ID': edr_req_ids[0]}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 429, 'headers': {'Retry-After': '1', 'X-Request-ID': edr_req_ids[1]}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 429, 'headers': {'Retry-After': '1', 'X-Request-ID': edr_req_ids[1]}},
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[1]}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}},
                       'status_code': 200, 'headers': {'X-Request-ID': edr_req_ids[1]}}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=local_edr_ids[0]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200,
                     headers={'X-Request-ID': edr_details_req_ids[0]})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=local_edr_ids[1]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200,
                     headers={'X-Request-ID': edr_details_req_ids[1]})

        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        check_queue = Queue(10)

        expected_result = []
        for i in range(2):
            tender_id = uuid.uuid4().hex
            award_id = uuid.uuid4().hex
            document_id = generate_doc_id()
            edr_ids = [str(random.randrange(10000000, 99999999)) for _ in range(2)]
            edrpou_codes_queue.put(Data(tender_id, award_id, edr_ids[i], "awards", None,
                                        {'meta': {'id': document_id, 'author': author,
                                                  'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))  # data
            expected_result.append(Data(tender_id, award_id, edr_ids[i], "awards", [local_edr_ids[i]],
                                        {"data": {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                              "id": document_id,
                                                              "version": version, 'author': author,
                                                              "sourceRequests": [
                                                                  'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                  edr_req_ids[i], edr_req_ids[i], edr_req_ids[i],
                                                                  edr_details_req_ids[i]]}}))  # result

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, check_queue, MagicMock())

        for result in expected_result:
            self.assertEquals(check_queue.get(), result)

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_proxy_client_402(self, mrequest, gevent_sleep):
        """First request returns Edr API returns to proxy 402 status code with messages."""
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        local_edr_ids = get_random_edr_ids(2)
        edr_req_id = [generate_request_id(), generate_request_id()]
        edr_details_req_id = [generate_request_id(), generate_request_id()]
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'json': {'errors': [{'description': [{'message': 'Payment required.', 'code': 5}]}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[0]}},  # pay for me
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[0]}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[0]}},
                      {'json': {'errors': [{'description': [{'message': 'Payment required.', 'code': 5}]}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[1]}},  # pay for me
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[1]}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[1]}}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=local_edr_ids[0]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200,  headers={'X-Request-ID': edr_details_req_id[0]})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=local_edr_ids[1]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200,  headers={'X-Request-ID': edr_details_req_id[1]})

        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        check_queue = Queue(10)

        expected_result = []
        for i in range(2):
            tender_id = uuid.uuid4().hex
            award_id = uuid.uuid4().hex
            document_id = generate_doc_id()
            edr_ids = [str(random.randrange(10000000, 99999999)) for _ in range(2)]
            edrpou_codes_queue.put(Data(tender_id, award_id, edr_ids[i], "awards", None,
                                        {'meta': {'id': document_id, 'author': author,
                                                  'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))  # data
            expected_result.append(Data(tender_id, award_id, edr_ids[i], "awards", [local_edr_ids[i]],
                                        {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                              "id": document_id, "version": version, 'author': author,
                                                              'sourceRequests': [
                                                                  'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                  edr_req_id[i], edr_req_id[i], edr_details_req_id[i]]}}))  # result

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, check_queue, MagicMock())

        for result in expected_result:
            self.assertEquals(check_queue.get(), result)
        self.assertIsNotNone(mrequest.request_history[3].headers['X-Client-Request-ID'])
        self.assertIsNotNone(mrequest.request_history[4].headers['X-Client-Request-ID'])

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_id(self, mrequest, gevent_sleep):
        """First and second response returns 403 status code. Tests retry for get_edr_id worker"""
        gevent_sleep.side_effect = custom_sleep
        local_edr_ids = get_random_edr_ids(1)
        edr_req_id = generate_request_id()
        edr_details_req_id = generate_request_id()
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id}},
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[0]}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}},
                       'status_code': 200, 'headers': {'X-Request-ID': edr_req_id}}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=local_edr_ids[0]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_id})

        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        check_queue = Queue(10)

        expected_result = []
        for i in range(1):
            tender_id = uuid.uuid4().hex
            award_id = uuid.uuid4().hex
            document_id = generate_doc_id()
            edr_ids = [str(random.randrange(10000000, 99999999)) for _ in range(2)]
            edrpou_codes_queue.put(Data(tender_id, award_id, edr_ids[i], "awards", None,
                                        {"meta": {"id": document_id, "author": author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))  # data
            expected_result.append(Data(tender_id, award_id, edr_ids[i], "awards", [local_edr_ids[i]],
                                        {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                              "id": document_id,
                                                              "version": version, 'author': author,
                                                              "sourceRequests": [
                                                                  'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                  edr_req_id, edr_req_id, edr_details_req_id]}}))  # result

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, check_queue, MagicMock())
        self.assertEquals(check_queue.get(), expected_result[0])
        self.assertIsNotNone(mrequest.request_history[3].headers['X-Client-Request-ID'])
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_get_edr_id_empty_response(self, mrequest, gevent_sleep):
        """Accept response with 404 status code and error message 'EDRPOU not found'. Check that tender_data
        is in upload_to_doc_service_queue."""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        edr_req_id = generate_request_id()
        document_id = generate_doc_id()
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     json={'errors': [{'description': [{"error": {"errorDetails": "Couldn't find this code in EDR.",
                                                                  "code": "notFound"},
                                                        "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}]}]},
                     status_code=404, headers={'X-Request-ID': edr_req_id})
        edrpou_codes_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edr_ids_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None,
                                    {'meta': {'id': document_id, 'author': author,
                                              'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, MagicMock())

        self.assertEquals(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id, item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[], file_content={"error": {"errorDetails": "Couldn't find this code in EDR.",
                                                                  "code": "notFound"},
                                                        "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                                 "id": document_id, "version": version, 'author': author,
                                                                 "sourceRequests": [
                                                                     'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                     edr_req_id]}}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)  # check that data not in edr_ids_queue
        self.assertEqual(mrequest.call_count, 1)  # Requests must call proxy once
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_id_empty_response(self, mrequest, gevent_sleep):
        """Accept 5 times response with status code 403 and error, then accept response with status code 404 and
        message 'EDRPOU not found'"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        award_id = uuid.uuid4().hex
        edr_req_id = generate_request_id()
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id}},
                      {'json': {'errors': [{'description': [{"error": {"errorDetails": "Couldn't find this code in EDR.",
                                                                       "code": "notFound"},
                                                             "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}]}]},
                       'status_code': 404,  'headers': {'X-Request-ID': edr_req_id}}])

        edrpou_codes_queue = Queue(10)
        edrpou_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None,
                                    {'meta': {'id': document_id, 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edrpou_ids_queue, upload_to_doc_service_queue, MagicMock())
        self.assertEquals(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id, item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[],
                              file_content={"error": {"errorDetails": "Couldn't find this code in EDR.",
                                                      "code": "notFound"},
                                            "meta": {"sourceDate": "2017-04-25T11:56:36+00:00", 'id': document_id, "version": version, 'author': author,
                                                     'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                        edr_req_id]}}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edrpou_ids_queue.qsize(), 0)  # check that data not in edr_ids_queue
        self.assertEqual(mrequest.call_count, 6)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[5].url, u'127.0.0.1:80/api/1.0/verify?id=123')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_id_value_error_mock(self, mrequest, gevent_sleep):
        """Accept 5 times response with status code 403 and error, then accept response with status code 404 and
         no message, then with same status and message 'EDRPOU not found'"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        award_id = uuid.uuid4().hex
        edr_req_id = generate_request_id()
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id}}])
        edrpou_codes_queue = Queue(10)
        edrpou_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None,
                                    {'meta': {'id': document_id, 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edrpou_ids_queue, upload_to_doc_service_queue, MagicMock())
        worker.get_edr_id_request = MagicMock(
            side_effect=[
                RetryException("text", MagicMock(status_code=404, json=MagicMock(return_value={
                          'errors': [{'description': [{"error": {"errorDetails": "Couldn't find this code in EDR.",
                                                                 "code": "notFound"},
                                                       "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}]}]}))),
            ]
        )
        self.assertEquals(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id, item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[],
                              file_content={"error": {"errorDetails": "Couldn't find this code in EDR.",
                                                      "code": "notFound"},
                                            "meta": {"sourceDate": "2017-04-25T11:56:36+00:00", 'id': document_id, "version": version, 'author': author,
                                                     'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                        edr_req_id]}}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edrpou_ids_queue.qsize(), 0)  # check that data not in edr_ids_queue
        self.assertEqual(mrequest.call_count, 1)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_get_edr_details_two_ids(self, mrequest, gevent_sleep):
        """Accept two ids in first request to /verify endpoint. Then make requests to /details endpoint for each id."""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        edr_req_id = generate_request_id()
        edr_details_req_id = [generate_request_id(), generate_request_id()]
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     json={'data': [{'x_edrInternalId': '321'}, {'x_edrInternalId': '322'}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}},
                     status_code=200, headers={'X-Request-ID': edr_req_id})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}},
                     status_code=200, headers={'X-Request-ID': edr_details_req_id[0]})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=322), json={'data': {'some': 'data'}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}},
                     status_code=200, headers={'X-Request-ID': edr_details_req_id[1]})
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None,
                                    {'meta': {'id': document_id, 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, MagicMock())

        self.assertEquals(upload_to_doc_service_queue.get(),
                        Data(tender_id=tender_id,
                             item_id=award_id,
                             code='123', item_name='awards',
                             edr_ids=[u'321', u'322'],
                             file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                                "id": "{}.{}.{}".format(document_id, 2, 1),
                                                                "version": version, 'author': author,
                                                                'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220', edr_req_id, edr_details_req_id[0]]}}))
        self.assertEquals(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id,
                              item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[u'321', u'322'],
                              file_content={'data': {'some': 'data'}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                                               "id": "{}.{}.{}".format(document_id, 2, 2),
                                                                               "version": version, 'author': author,
                                                                               'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220', edr_req_id, edr_details_req_id[1]]}}))

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 3)  # 1 request to /verify and two requests to /details
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/322')
        self.assertIsNotNone(mrequest.request_history[2].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_details_two_ids(self, mrequest, gevent_sleep):
        """Accept two ids in /verify request. Then accept 200 response for first id, and 403 and 200
        responses for second id. Check retry_get_edr_details job"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        edr_req_id = generate_request_id()
        edr_details_req_id = [generate_request_id(), generate_request_id(), generate_request_id()]
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     json={'data': [{'x_edrInternalId': '321'}, {'x_edrInternalId': '322'}],
                           "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_req_id})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_id[0]})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=322),
                     [{'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[1]}},
                      {'json': {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_details_req_id[2]}}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None,
                                    {'meta': {'id': document_id, 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, MagicMock())

        self.assertEquals(upload_to_doc_service_queue.get(),
                                Data(tender_id=tender_id, item_id=award_id,
                                     code='123', item_name='awards',
                                     edr_ids=[u'321', u'322'],
                                     file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                                        "id": '{}.{}.{}'.format(document_id, 2, 1),
                                                                        "version": version, 'author': author,
                                                                        'sourceRequests': [
                                                                            'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                            edr_req_id, edr_details_req_id[0]]}}))
        self.assertEquals(upload_to_doc_service_queue.get(),
                                Data(tender_id=tender_id,
                                     item_id=award_id,
                                     code='123', item_name='awards', edr_ids=[],
                                     file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                                        "id": '{}.{}.{}'.format(document_id, 2, 2),
                                                                        "version": version, 'author': author,
                                                                        'sourceRequests': [
                                                                            'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                            edr_req_id, edr_details_req_id[1],
                                                                            edr_details_req_id[2]]}}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 4)  # processed 4 requests
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/322')
        self.assertIsNotNone(mrequest.request_history[2].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[3].url, u'127.0.0.1:80/api/1.0/details/322')
        self.assertIsNotNone(mrequest.request_history[3].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_details_value_error_2(self, mrequest, gevent_sleep):
        """Mock returning first malformed Error 404, then proper Error 404, then the correct response"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        award_id = uuid.uuid4().hex
        edr_req_id = generate_request_id()
        edr_details_req_id = generate_request_id()
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        edrpou_codes_queue = Queue(10)
        edrpou_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edrpou_ids_queue, upload_to_doc_service_queue, MagicMock())
        worker.retry_edr_ids_queue.put(Data(tender_id, award_id, '123', "awards", ['111'],
                                    {'meta': {'id': document_id, 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        worker.get_edr_details_request = MagicMock(
            side_effect=[
                RetryException("text", MagicMock(status_code=404, json=MagicMock(side_effect=ValueError))),
                RetryException("text", MagicMock(status_code=404, json=MagicMock(return_value={
                          'errors': [{'description': [{"error": {"errorDetails": "Couldn't find this code in EDR.",
                                                                 "code": "notFound"},
                                                       "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}]}]}))),
                MagicMock(status_code=200, json=MagicMock(return_value={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}), headers={'X-Request-ID': edr_details_req_id})
            ]
        )
        self.assertEquals(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id, item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[],
                              file_content={'data': {},
                                            "meta": {"sourceDate": "2017-04-25T11:56:36+00:00", 'id': document_id, "version": version, 'author': author,
                                                     'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                        edr_details_req_id]}}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edrpou_ids_queue.qsize(), 0)  # check that data not in edr_ids_queue
        self.assertEqual(mrequest.call_count, 0)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_job_get_edr_id_dead(self, mrequest, gevent_sleep):
        """Accept dict instead of list in first response to /verify endpoint. Check that worker get up"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        edr_req_id = [generate_request_id(), generate_request_id()]
        edr_details_req_id = generate_request_id()
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     [{'json': {'data': {'x_edrInternalId': '321'}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[0]}},  # data contains dict, instead of list
                      {'json': {'data': [{'x_edrInternalId': '321'}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[1]}}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_id})
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None,
                                    {'meta': {'id': document_id, 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, MagicMock())

        self.assertEquals(upload_to_doc_service_queue.get(),
                                Data(tender_id=tender_id, item_id=award_id,
                                     code='123', item_name='awards',
                                     edr_ids=[u'321'], file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                                                          "id": document_id, "version": version, 'author': author,
                                                                                          'sourceRequests': [
                                                                                              'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                                              edr_req_id[0],
                                                                                              edr_req_id[1],
                                                                                              edr_details_req_id]}}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 3)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[2].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_job_retry_get_edr_id_dead(self, mrequest, gevent_sleep):
        """Accept dict instead of list in first response to /verify endpoint. Check that worker get up"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        edr_req_id = [generate_request_id(), generate_request_id(), generate_request_id()]
        edr_details_req_id = generate_request_id()
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     [{'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[0]}},
                      {'json': {'data': {'x_edrInternalId': '321'}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[1]}},  # data contains dict, instead of list
                      {'json': {'data': [{'x_edrInternalId': '321'}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[2]}}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_id})
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None,
                                    {'meta': {'id': document_id, 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, MagicMock())

        self.assertEquals(upload_to_doc_service_queue.get(),
                            Data(tender_id=tender_id, item_id=award_id,
                                     code='123', item_name='awards',
                                     edr_ids=[u'321'], file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                                                          "id": document_id, "version": version, 'author': author,
                                                                                          'sourceRequests': [
                                                                                              'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                                              edr_req_id[0],
                                                                                              edr_req_id[1],
                                                                                              edr_req_id[2],
                                                                                              edr_details_req_id]}}))

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 4)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[3].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[3].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_job_get_edr_details_dead(self, mrequest, gevent_sleep):
        """Accept list instead of dict in response to /details/{id} endpoint. Check that worker get up"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        edr_req_id = generate_request_id()
        edr_details_req_id = [generate_request_id(), generate_request_id()]
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url), json={'data': [{'x_edrInternalId': '321'}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_req_id})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321),
                     [{'json': [], 'status_code': 200, 'headers': {'X-Request-ID': edr_details_req_id[0]}},  # list instead of dict in data
                      {'json': {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_details_req_id[1]}}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None,
                                    {'meta': {'id': document_id, 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, MagicMock())

        self.assertEquals(upload_to_doc_service_queue.get(),
                                Data(tender_id=tender_id, item_id=award_id,
                                     code='123', item_name='awards',
                                     edr_ids=[],
                                     file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                                        "id": document_id, "version": version, 'author': author,
                                                                        'sourceRequests': [
                                                                            'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                            edr_req_id, edr_details_req_id[0],
                                                                            edr_details_req_id[1]]}}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 3)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[2].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_job_retry_get_edr_details_dead(self, mrequest, gevent_sleep):
        """Accept list instead of dict in response to /details/{id} endpoint. Check that worker get up"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        edr_req_id = generate_request_id()
        edr_details_req_id = [generate_request_id(), generate_request_id(), generate_request_id()]
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url), json={'data': [{'x_edrInternalId': '321'}],
                            "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_req_id})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321),
                     [{'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[0]}},
                      {'json': [], 'status_code': 200, 'headers': {'X-Request-ID': edr_details_req_id[1]}},  # list instead of dict in data
                      {'json': {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_details_req_id[2]}}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None,
                                    {'meta': {'id': document_id, 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, MagicMock())

        self.assertEquals(upload_to_doc_service_queue.get(),
                                Data(tender_id=tender_id, item_id=award_id,
                                     code='123', item_name='awards',
                                     edr_ids=[],
                                     file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                                        "id": document_id, "version": version, 'author': author,
                                                                        'sourceRequests': [
                                                                            'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                            edr_req_id, edr_details_req_id[0],
                                                                            edr_details_req_id[1],
                                                                            edr_details_req_id[2]]}}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 4)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[2].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_details(self, mrequest, gevent_sleep):
        """Accept 6 times errors in response while requesting /details"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        edr_req_id = generate_request_id()
        edr_details_req_id = [generate_request_id(), generate_request_id(), generate_request_id(), generate_request_id(), generate_request_id(), generate_request_id(), generate_request_id()]
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url), json={'data': [{'x_edrInternalId': '321'}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200,  headers={'X-Request-ID': edr_req_id})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321),
                     [{'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[0]}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[1]}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[2]}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[3]}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[4]}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[5]}},
                      {'json': {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_details_req_id[6]}}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None,
                                    {'meta': {'id': document_id, 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, MagicMock())

        self.assertEquals(upload_to_doc_service_queue.get(),
                                Data(tender_id=tender_id, item_id=award_id,
                                     code='123', item_name='awards',
                                     edr_ids=[],
                                     file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                                        "id": document_id, "version": version, 'author': author,
                                                                        'sourceRequests': [
                                                                            'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                            edr_req_id, edr_details_req_id[0],
                                                                            edr_details_req_id[6]]
                                                                        }}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 8)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])


    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_details_no_edr_ids(self, mrequest, gevent_sleep):
        """Test that retry_get_edr_ids removes from retry_queue items with empty edr_ids lists"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edr_ids_queue.put(Data(tender_id, award_id, '123', "awards", [],
                                    {'meta': {'id': document_id, 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, MagicMock())
        with self.assertRaises(Empty):
            upload_to_doc_service_queue.get(timeout=1)
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 0)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_details_two_edr_ids_one_success(self, mrequest, gevent_sleep):
        """Test that retry_get_edr_details correctly processes case of one working, other not and does not delete"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        edr_details_req_id = [generate_request_id(), generate_request_id(), generate_request_id(),
                              generate_request_id(), generate_request_id(), generate_request_id(),
                              generate_request_id()]
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321),
                     [{'json': {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200,
                       'headers': {'X-Request-ID': edr_details_req_id[0]}}
                      ])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=322),
                     [{'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[1]}},
                     {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[2]}},
                     {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[3]}},
                     {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[4]}},
                     {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[5]}},
                     {'json': {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_details_req_id[6]}}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, MagicMock())
        worker.retry_edr_ids_queue.put(Data(tender_id, award_id, '123', "awards", ['321', '322'],
                                            {'meta': {'id': document_id, 'author': author,
                                                      'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        self.assertEquals(upload_to_doc_service_queue.get(),
                          Data(tender_id=tender_id, item_id=award_id,
                               code='123', item_name='awards',
                               edr_ids=['322'],
                               file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                                  "id": document_id, "version": version,
                                                                  'author': author,
                                                                  'sourceRequests': [
                                                                      'req-db3ed1c6-9843-415f-92c9-7d4b08d39220', edr_details_req_id[0]]
                                                                  }}))
        self.assertEquals(upload_to_doc_service_queue.get(),
                          Data(tender_id=tender_id, item_id=award_id,
                               code='123', item_name='awards',
                               edr_ids=[],
                               file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                                  "id": document_id, "version": version,
                                                                  'author': author,
                                                                  'sourceRequests': [
                                                                      'req-db3ed1c6-9843-415f-92c9-7d4b08d39220', edr_details_req_id[0], edr_details_req_id[6]]
                                                                  }}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 7)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_5_times_get_edr_id(self, mrequest, gevent_sleep):
        """Accept 6 times errors in response while requesting /verify"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        edr_details_req_id = generate_request_id()
        edr_req_id = [generate_request_id(), generate_request_id(), generate_request_id(),
                      generate_request_id(), generate_request_id(), generate_request_id(),
                      generate_request_id()]
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     [{'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[0]}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[1]}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[2]}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[3]}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[4]}},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[5]}},
                      {'json': {'data': [{'x_edrInternalId': '321'}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[6]}}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_id})
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None,
                                    {'meta': {'id': document_id, 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, MagicMock())
        self.assertEquals(upload_to_doc_service_queue.get(),
                                Data(tender_id=tender_id, item_id=award_id,
                                     code='123', item_name='awards',
                                     edr_ids=[u'321'],
                                     file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                                        "id": document_id, "version": version, 'author': author,
                                                                        'sourceRequests': [
                                                                            'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                            edr_req_id[0], edr_req_id[6],
                                                                            edr_details_req_id]
                                                                        }}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 8)  # processing 8 requests
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')  # check first url
        self.assertEqual(mrequest.request_history[6].url, u'127.0.0.1:80/api/1.0/verify?id=123')  # check 7th url
        self.assertEqual(mrequest.request_history[7].url, u'127.0.0.1:80/api/1.0/details/321')  # check 8th url
        self.assertIsNotNone(mrequest.request_history[7].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_timeout(self, mrequest, gevent_sleep):
        """Accept 'Gateway Timeout Error'  while requesting /verify, then accept 200 status code. Accept
        'Gateway Timeout Error' while requesting /details/{id}, then accept 200 status code."""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        edr_details_req_id = [generate_request_id(), generate_doc_id()]
        edr_req_id = [generate_request_id(), generate_request_id()]
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     [{'json': {'errors': [{'description': [{u'message': u'Gateway Timeout Error'}]}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[0]}},
                      {'json': {'data': [{'x_edrInternalId': 321}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[1]}}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321),
                     [{'json': {'errors': [{'description': [{u'message': u'Gateway Timeout Error'}]}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[0]}},
                      {'json': {'data': {'id': 321}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_details_req_id[1]}}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None,
                                    {'meta': {'id': document_id, 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, MagicMock())
        self.assertEquals(upload_to_doc_service_queue.get(),
                                Data(tender_id=tender_id, item_id=award_id,
                                     code='123', item_name='awards',
                                     edr_ids=[],
                                     file_content={'data': {'id': 321}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                                                "id": document_id, "version": version, 'author': author,
                                                                                 'sourceRequests': [
                                                                                     'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                                     edr_req_id[0], edr_req_id[1],
                                                                                     edr_details_req_id[0],
                                                                                     edr_details_req_id[1]]
                                                                                 }}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 4)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[2].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[3].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[3].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_identifier_id_type(self, mrequest, gevent_sleep):
        """Create filter_tenders and edr_handler workers. Test when identifier.id is type int (not str)."""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        bid_id = uuid.uuid4().hex
        edr_details_req_id = generate_request_id()
        edr_req_id = generate_request_id()

        #  create queues
        filtered_tender_ids_queue = Queue(10)
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        filtered_tender_ids_queue.put(tender_id)

        # create workers and responses
        client = MagicMock()
        client.request.return_value = ResponseMock({'X-Request-ID': 'req-db3ed1c6-9843-415f-92c9-7d4b08d39220'},
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': {'status': "active.pre-qualification",
                               'id': tender_id,
                               'procurementMethodType': 'aboveThresholdEU',
                               'awards': [{'id': award_id,
                                           'status': 'pending',
                                           'bid_id': bid_id,
                                           'suppliers': [{'identifier': {
                                             'scheme': 'UA-EDR',
                                             'id': 14360570}  # int instead of str type
                                         }]}, ]}}))
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url), [{'json': {'data': [{'x_edrInternalId': 321}],
                     "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_id}}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), [{'json': {'data': {'id': 321},
                     "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_details_req_id}}])
        filter_tenders_worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, {})
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, MagicMock())

        obj = upload_to_doc_service_queue.get()
        self.assertEqual(obj.tender_id, tender_id)
        self.assertEqual(obj.item_id, award_id)
        self.assertEqual(obj.code, '14360570')
        self.assertEqual(obj.item_name, 'awards')
        self.assertEqual(obj.edr_ids, [321])
        self.assertEqual(obj.file_content['data'], {'id': 321})
        self.assertEqual(obj.file_content['meta']['sourceDate'], "2017-04-25T11:56:36+00:00")
        self.assertIsNotNone(obj.file_content['meta']['id'])
        self.assertEqual(obj.file_content['meta']['version'], version)
        self.assertEqual(obj.file_content['meta']['author'], author)
        self.assertEqual(obj.file_content['meta']['sourceRequests'], ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220', edr_req_id, edr_details_req_id])

        worker.shutdown()
        filter_tenders_worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(filtered_tender_ids_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_processing_items(self, mrequest, gevent_sleep):
        """Return list of objects from EDR. Check number of edr_ids in processing_items."""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        qualification_id = uuid.uuid4().hex
        document_ids = [generate_doc_id(), generate_doc_id()]
        edr_req_id = [generate_request_id(), generate_request_id()]
        edr_details_req_id = [generate_request_id(), generate_request_id(), generate_request_id(), generate_request_id(), generate_request_id()]
        processing_items = {}
        award_key = '{}_{}'.format(tender_id, award_id)
        qualification_key = '{}_{}'.format(tender_id, qualification_id)
        data_1 = Data(tender_id, award_id, '123', "awards", [321, 322],
                      {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                            "id": "{}.{}.{}".format(document_ids[0], 2, 1),
                                            "version": version, 'author': author,
                                            'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                               edr_req_id[0], edr_details_req_id[0]]}})
        data_2 = Data(tender_id, award_id, '123', "awards", [321, 322],
                      {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                            "id": "{}.{}.{}".format(document_ids[0], 2, 2),
                                            "version": version, 'author': author,
                                            'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                               edr_req_id[0], edr_details_req_id[1]]}})
        data_3 = Data(tender_id, qualification_id, '124', 'qualifications', [323, 324, 325],
                      {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                            "id": "{}.{}.{}".format(document_ids[1], 3, 1),
                                             "version": version, 'author': author,
                                            'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                               edr_req_id[1], edr_details_req_id[2]]}})
        data_4 = Data(tender_id, qualification_id, '124', 'qualifications', [323, 324, 325],
                      {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                            "id": "{}.{}.{}".format(document_ids[1], 3, 2),
                                            "version": version, 'author': author,
                                            'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                               edr_req_id[1], edr_details_req_id[3]]}})
        data_5 = Data(tender_id, qualification_id, '124', 'qualifications', [323, 324, 325],
                      {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                            "id": "{}.{}.{}".format(document_ids[1], 3, 3),
                                            "version": version, 'author': author,
                                            'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                               edr_req_id[1], edr_details_req_id[4]]}})

        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     [{'json': {'data': [{'x_edrInternalId': 321}, {'x_edrInternalId': 322}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[0]}},
                      {'json': {'data': [{'x_edrInternalId': 323}, {'x_edrInternalId': 324}, {'x_edrInternalId': 325}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[1]}}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_id[0]})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=322), json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_id[1]})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=323), json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_id[2]})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=324), json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_id[3]})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=325), json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_id[4]})

        #  create queues
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None,
                                    {'meta': {'id': document_ids[0], 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        edrpou_codes_queue.put(Data(tender_id, qualification_id, '124', 'qualifications', None,
                                    {'meta': {'id': document_ids[1], 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue,
                                  upload_to_doc_service, processing_items)

        for data in [data_1, data_2, data_3, data_4, data_5]:
            self.assertEquals(upload_to_doc_service.get(), data)

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(processing_items[award_key], 2)
        self.assertEqual(processing_items[qualification_key], 3)
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertIsNotNone(mrequest.request_history[2].headers['X-Client-Request-ID'])
        self.assertIsNotNone(mrequest.request_history[4].headers['X-Client-Request-ID'])
        self.assertIsNotNone(mrequest.request_history[5].headers['X-Client-Request-ID'])
        self.assertIsNotNone(mrequest.request_history[6].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_wrong_ip(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        edr_req_id = [generate_request_id(), generate_request_id()]
        edr_details_req_id = [generate_request_id(), generate_request_id()]
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     [{'json': {'errors': [{'description': [{u'message': u'Forbidden'}]}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[0]}},
                      {'json': {'data': [{'x_edrInternalId': 321}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[1]}}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321),
                     [{'json': {'errors': [{'description': [{u'message': u'Forbidden'}]}]}, 'status_code': 403, 'headers': {'X-Request-ID': edr_details_req_id[0]}},
                      {'json': {'data': {'id': 321}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_details_req_id[1]}}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None,
                                    {'meta': {'id': document_id, 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, {})
        self.assertEquals(upload_to_doc_service_queue.get(),
                                Data(tender_id=tender_id, item_id=award_id,
                                     code='123', item_name='awards',
                                     edr_ids=[],
                                     file_content={'data': {'id': 321},
                                                   "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                            "id": document_id, "version": version, 'author': author,
                                                            'sourceRequests': [
                                                                'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                edr_req_id[0], edr_req_id[1],
                                                                edr_details_req_id[0],
                                                                edr_details_req_id[1]]}}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 4)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[2].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[3].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertIsNotNone(mrequest.request_history[3].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_edrpou_codes_queue_loop_exit(self, mrequest, gevent_sleep):
        """ Test LoopExit for edrpou_codes_queue """
        document_ids = [generate_doc_id(), generate_doc_id()]
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        local_edr_ids = get_random_edr_ids(2)
        edr_req_id = [generate_request_id(), generate_request_id()]
        edr_details_req_id = [generate_request_id(), generate_request_id()]
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'json': {'data': [{'x_edrInternalId': local_edr_ids[0]}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[0]}},
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[1]}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, 'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[1]}}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=local_edr_ids[0]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_id[0]})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=local_edr_ids[1]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_id[1]})

        edrpou_codes_queue = MagicMock()
        edr_ids_queue = Queue(10)
        check_queue = Queue(10)

        expected_result = []
        edrpou_codes_queue_list = [LoopExit()]
        for i in range(2):
            tender_id = uuid.uuid4().hex
            award_id = uuid.uuid4().hex
            edr_ids = [str(random.randrange(10000000, 99999999)) for _ in range(2)]
            edrpou_codes_queue_list.append(Data(tender_id, award_id, edr_ids[i], "awards", None,
                    {'meta': {'id': document_ids[i], 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))  # data
            expected_result.append(Data(tender_id, award_id, edr_ids[i], "awards", [local_edr_ids[i]],
                                        {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00", "id": document_ids[i],
                                                              "version": version, 'author': author,
                                                              'sourceRequests': [
                                                                  'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                  edr_req_id[i], edr_details_req_id[i]]}}))  # result

        edrpou_codes_queue.peek.side_effect = generate_answers(answers=edrpou_codes_queue_list,
                                                               default=LoopExit())

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, check_queue, MagicMock())

        for result in expected_result:
            self.assertEquals(check_queue.get(), result)

        self.assertEqual(mrequest.request_history[0].url,
                         u'127.0.0.1:80/api/1.0/verify?id={edr_code}'.format(edr_code=expected_result[0].code))
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[2].url,
                         u'127.0.0.1:80/api/1.0/verify?id={edr_code}'.format(edr_code=expected_result[1].code))
        self.assertIsNotNone(mrequest.request_history[3].headers['X-Client-Request-ID'])

        worker.shutdown()
        self.assertEqual(mrequest.call_count, 4)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_edrpou_codes_queue_loop_exit(self, mrequest, gevent_sleep):
        """ Test LoopExit for retry_edrpou_codes_queue """
        document_ids = [generate_doc_id(), generate_doc_id()]
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        local_edr_ids = get_random_edr_ids(2)
        edr_req_id = [generate_request_id(), generate_request_id()]
        edr_details_req_id = [generate_request_id(), generate_request_id()]
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'json': {'data': [{'x_edrInternalId': local_edr_ids[0]}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}},
                       'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[0]}},
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[1]}], "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}},
                       'status_code': 200, 'headers': {'X-Request-ID': edr_req_id[1]}}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=local_edr_ids[0]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200,
                     headers={'X-Request-ID': edr_details_req_id[0]})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=local_edr_ids[1]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200,
                     headers={'X-Request-ID': edr_details_req_id[1]})

        edrpou_codes_queue = Queue(1)
        edr_ids_queue = Queue(10)
        check_queue = Queue(10)
        edrpou_codes_queue_list = [LoopExit()]

        expected_result = []
        for i in range(2):
            tender_id = uuid.uuid4().hex
            award_id = uuid.uuid4().hex
            edr_ids = [str(random.randrange(10000000, 99999999)) for _ in range(2)]
            edrpou_codes_queue_list.append(Data(tender_id, award_id, edr_ids[i], "awards", None,
                                                {"meta": {"id": document_ids[i], 'author': author, 'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))  # data
            expected_result.append(Data(tender_id, award_id, edr_ids[i], "awards", [local_edr_ids[i]],
                                        {'data': {},
                                         "meta": {"sourceDate": "2017-04-25T11:56:36+00:00", "id": document_ids[i],
                                                  "version": version, 'author': author,
                                                  'sourceRequests': [
                                                      'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                      edr_req_id[i], edr_details_req_id[i]]}}))  # result

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, check_queue, MagicMock())
        worker.retry_edrpou_codes_queue = MagicMock()
        worker.retry_edrpou_codes_queue.peek.side_effect = generate_answers(answers=edrpou_codes_queue_list,
                                                                           default=LoopExit())

        for result in expected_result:
            self.assertEquals(check_queue.get(), result)

        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id={edr_code}'.format(edr_code=expected_result[0].code))
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/verify?id={edr_code}'.format(edr_code=expected_result[1].code))
        self.assertIsNotNone(mrequest.request_history[3].headers['X-Client-Request-ID'])

        worker.shutdown()
        self.assertEqual(mrequest.call_count, 4)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_edr_ids_queue_loop_exit(self, mrequest, gevent_sleep):
        """ Test LoopExit for edr_ids_queue """
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        document_ids = [generate_doc_id(), generate_doc_id()]
        local_edr_ids = get_random_edr_ids(2)
        edr_req_id = generate_request_id()
        edr_details_req_id = [generate_request_id(), generate_request_id()]
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=local_edr_ids[0]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200,
                     headers={'X-Request-ID': edr_details_req_id[0]})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=local_edr_ids[1]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200,
                     headers={'X-Request-ID': edr_details_req_id[1]})

        edrpou_codes_queue = Queue(10)
        edr_ids_queue = MagicMock()
        check_queue = Queue(10)

        expected_result = []
        for i in range(2):
            tender_id = uuid.uuid4().hex
            award_id = uuid.uuid4().hex
            edr_ids = [str(random.randrange(10000000, 99999999)) for _ in range(2)]
            expected_result.append(Data(tender_id, award_id, edr_ids[i], "awards", [local_edr_ids[i]],
                                        {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00", "id": document_ids[i],
                                                              "version": version, 'author': author,
                                                              'sourceRequests': [
                                                                  'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                  edr_req_id, edr_details_req_id[i]]}}))  # result

        edr_ids_queue.peek.side_effect = generate_answers(
            answers=[LoopExit(),
                     Data(tender_id=expected_result[0].tender_id, item_id=expected_result[0].item_id, code=expected_result[0].code,
                          item_name='awards', edr_ids=[local_edr_ids[0]],
                          file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                             "id": document_ids[0], 'author': author,
                                                             'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220', edr_req_id]}}),
                     Data(tender_id=expected_result[1].tender_id, item_id=expected_result[1].item_id, code=expected_result[1].code,
                          item_name='awards', edr_ids=[local_edr_ids[1]],
                          file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                             "id": document_ids[1], 'author': author,
                                                             'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220', edr_req_id]}})],
            default=LoopExit())

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, check_queue, MagicMock())

        for result in expected_result:
            self.assertEquals(check_queue.get(), result)

        self.assertEqual(mrequest.request_history[0].url,
                         u'127.0.0.1:80/api/1.0/details/{id}'.format(id=local_edr_ids[0]))
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[1].url,
                         u'127.0.0.1:80/api/1.0/details/{id}'.format(id=local_edr_ids[1]))
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])

        worker.shutdown()
        self.assertEqual(mrequest.call_count, 2)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_edr_ids_queue_loop_exit(self, mrequest, gevent_sleep):
        """ Test LoopExit for retry_edr_ids_queue """
        gevent_sleep.side_effect = custom_sleep
        document_ids = [generate_doc_id(), generate_doc_id()]
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        local_edr_ids = get_random_edr_ids(2)
        edr_req_id = generate_request_id()
        edr_details_req_id = [generate_request_id(), generate_request_id()]
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url,
                                         id=local_edr_ids[0]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_id[0]})
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url,
                                         id=local_edr_ids[1]),
                     json={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00"}}, status_code=200, headers={'X-Request-ID': edr_details_req_id[1]})

        edrpou_codes_queue = Queue(1)
        edr_ids_queue = Queue(1)
        check_queue = Queue(10)

        expected_result = []
        for i in range(2):
            tender_id = uuid.uuid4().hex
            award_id = uuid.uuid4().hex
            edr_ids = [str(random.randrange(10000000, 99999999)) for _ in range(2)]
            expected_result.append(Data(tender_id, award_id, edr_ids[i], "awards", [],
                                        {'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00", "id": document_ids[i],
                                                              "version": version, 'author': author,
                                                              'sourceRequests': [
                                                                  'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                                  edr_req_id, edr_details_req_id[i]]
                                                              }}))  # result

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, check_queue, MagicMock())
        worker.retry_edr_ids_queue = MagicMock()
        worker.retry_edr_ids_queue.peek.side_effect = generate_answers(
            answers=[LoopExit(),
                     Data(tender_id=expected_result[0].tender_id, item_id=expected_result[0].item_id, code=expected_result[0].code,
                          item_name='awards', edr_ids=[local_edr_ids[0]],
                          file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                             "id": document_ids[0], 'author': author,
                                                             'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220', edr_req_id]}}),
                     Data(tender_id=expected_result[1].tender_id, item_id=expected_result[1].item_id, code=expected_result[1].code,
                          item_name='awards', edr_ids=[local_edr_ids[1]],
                          file_content={'data': {}, "meta": {"sourceDate": "2017-04-25T11:56:36+00:00",
                                                             "id": document_ids[1], 'author': author,
                                                             'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220', edr_req_id]}})],
            default=LoopExit())

        for result in expected_result:
            self.assertEquals(check_queue.get(), result)

        self.assertEqual(mrequest.request_history[0].url,
                         u'127.0.0.1:80/api/1.0/details/{id}'.format(id=local_edr_ids[0]))
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[1].url,
                         u'127.0.0.1:80/api/1.0/details/{id}'.format(id=local_edr_ids[1]))
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])

        worker.shutdown()
        self.assertEqual(mrequest.call_count, 2)
