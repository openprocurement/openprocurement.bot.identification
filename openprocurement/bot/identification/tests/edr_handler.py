# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import uuid
import unittest
import datetime
import requests_mock
import requests
import random

from time import sleep
from gevent.queue import Queue
from mock import patch, MagicMock
from munch import munchify

from openprocurement.bot.identification.databridge.edr_handler import EdrHandler
from openprocurement.bot.identification.databridge.filter_tender import FilterTenders
from openprocurement.bot.identification.databridge.utils import Data
from openprocurement.bot.identification.tests.utils import custom_sleep
from openprocurement.bot.identification.client import ProxyClient


def get_random_edr_ids(count=1):
    return [str(random.randrange(10000000, 99999999)) for _ in range(count)]


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
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        local_edr_ids = get_random_edr_ids(2)
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'json': {'data': [{'x_edrInternalId': local_edr_ids[0]}]}, 'status_code': 200},
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[1]}]}, 'status_code': 200}])

        edrpou_codes_queue = Queue(10)
        check_queue = Queue(10)

        expected_result = []
        for i in range(2):
            tender_id = uuid.uuid4().hex
            award_id = uuid.uuid4().hex
            edr_ids = [str(random.randrange(10000000, 99999999)) for _ in range(2)]
            edrpou_codes_queue.put(Data(tender_id, award_id, edr_ids[i], "awards", None, None))  # data
            expected_result.append(Data(tender_id, award_id, edr_ids[i], "awards", [local_edr_ids[i]], None))  # result

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, check_queue, MagicMock())

        for result in expected_result:
            self.assertEqual(check_queue.get(), result)

        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id={edr_code}'.format(edr_code=expected_result[0].code))
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/verify?id={edr_code}'.format(edr_code=expected_result[1].code))

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')
        self.assertEqual(mrequest.call_count, 4)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_proxy_client_401(self, mrequest, gevent_sleep):
        """ After 401 need restart worker """
        local_edr_ids = get_random_edr_ids(2)
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'text': '', 'status_code': 401},
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[0]}]}, 'status_code': 200},
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[1]}]}, 'status_code': 200}])

        edrpou_codes_queue = Queue(10)
        check_queue = Queue(10)
        expected_result = []
        for i in range(2):
            tender_id = uuid.uuid4().hex
            award_id = uuid.uuid4().hex
            edr_ids = [str(random.randrange(10000000, 99999999)) for _ in range(2)]
            edrpou_codes_queue.put(Data(tender_id, award_id, edr_ids[i], "awards", None, None))  # data
            expected_result.append(Data(tender_id, award_id, edr_ids[i], "awards", [local_edr_ids[i]], None))  # result

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, check_queue, MagicMock())

        for result in expected_result:
            self.assertEqual(check_queue.get(), result)

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_proxy_client_429(self, mrequest, gevent_sleep):
        """Accept 429 status code in first request with header 'Retry-After'"""
        local_edr_ids = get_random_edr_ids(2)
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'text': '', 'status_code': 429, 'headers': {'Retry-After': '10'}},
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[0]}]},'status_code': 200},
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[1]}]}, 'status_code': 200}])


        edrpou_codes_queue = Queue(10)
        check_queue = Queue(10)

        expected_result = []
        for i in range(2):
            tender_id = uuid.uuid4().hex
            award_id = uuid.uuid4().hex
            edr_ids = [str(random.randrange(10000000, 99999999)) for _ in range(2)]
            edrpou_codes_queue.put(Data(tender_id, award_id, edr_ids[i], "awards", None, None))  # data
            expected_result.append(Data(tender_id, award_id, edr_ids[i], "awards", [local_edr_ids[i]], None))  # result

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, check_queue, MagicMock())

        for result in expected_result:
            self.assertEqual(check_queue.get(), result)

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_proxy_client_402(self, mrequest, gevent_sleep):
        """First request returns 402 status code."""
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        local_edr_ids = get_random_edr_ids(2)
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'text': '', 'status_code': 402},  # pay for me
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[0]}]}, 'status_code': 200},
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[1]}]}, 'status_code': 200}])

        edrpou_codes_queue = Queue(10)
        check_queue = Queue(10)

        expected_result = []
        for i in range(2):
            tender_id = uuid.uuid4().hex
            award_id = uuid.uuid4().hex
            edr_ids = [str(random.randrange(10000000, 99999999)) for _ in range(2)]
            edrpou_codes_queue.put(Data(tender_id, award_id, edr_ids[i], "awards", None, None))  # data
            expected_result.append(Data(tender_id, award_id, edr_ids[i], "awards", [local_edr_ids[i]], None))  # result

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue,
                                  check_queue, MagicMock())

        for result in expected_result:
            self.assertEqual(check_queue.get(), result)

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_id(self, mrequest, gevent_sleep):
        """First and second response returns 402 status code. Tests retry for get_edr_id worker"""
        gevent_sleep.side_effect = custom_sleep
        local_edr_ids = get_random_edr_ids(1)
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'text': '', 'status_code': 402},
                      {'text': '', 'status_code': 402},
                      {'json': {'data': [{'x_edrInternalId': local_edr_ids[0]}]}, 'status_code': 200}])

        edrpou_codes_queue = Queue(10)
        check_queue = Queue(10)

        expected_result = []
        for i in range(1):
            tender_id = uuid.uuid4().hex
            award_id = uuid.uuid4().hex
            edr_ids = [str(random.randrange(10000000, 99999999)) for _ in range(2)]
            edrpou_codes_queue.put(Data(tender_id, award_id, edr_ids[i], "awards", None, None))  # data
            expected_result.append(Data(tender_id, award_id, edr_ids[i], "awards", [local_edr_ids[i]], None))  # result

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue,
                                  check_queue, MagicMock())

        self.assertEqual(check_queue.get(), expected_result[0])
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0, 'Queue must be empty')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_get_edr_id_empty_response(self, mrequest, gevent_sleep):
        """Accept response with 403 status code and error message 'EDRPOU not found'. Check that tender_data
        is in upload_to_doc_service_queue."""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     json={'errors': [{'description': [{'message': 'EDRPOU not found'}]}]}, status_code=403)
        edrpou_codes_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edr_ids_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None, None))

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)

        self.assertEqual(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id, item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[], file_content={'error': "Couldn't find this code in EDR."}))

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)  # check that data not in edr_ids_queue
        self.assertEqual(mrequest.call_count, 1)  # Requests must call proxy once
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_id_empty_response(self, mrequest, gevent_sleep):
        """Accept 5 times response with status code 403 and error, then accept response with status code 403 and
        message 'EDRPOU not found'"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'json': {'errors': [{'description': ''}]}, 'status_code': 403},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403},
                      {'json': {'errors': [{'description': [{'message': 'EDRPOU not found'}]}]}, 'status_code': 403}])

        edrpou_codes_queue = Queue(10)
        edrpou_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edrpou_ids_queue, upload_to_doc_service_queue)

        self.assertEqual(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id, item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[], file_content={'error': "Couldn't find this code in EDR."}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edrpou_ids_queue.qsize(), 0)  # check that data not in edr_ids_queue
        self.assertEqual(mrequest.call_count, 6)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[5].url, u'127.0.0.1:80/api/1.0/verify?id=123')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_get_edr_details_two_ids(self, mrequest, gevent_sleep):
        """Accept two ids in first request to /verify endpoint. Then make requests to /details endpoint for each id."""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     json={'data': [{'x_edrInternalId': '321'}, {'x_edrInternalId': '322'}]}, status_code=200)
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), json={'data': {}}, status_code=200)
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=322), json={'data': {'some': 'data'}}, status_code=200)
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)

        self.assertEqual(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id,
                              item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[u'321', u'322'], file_content={}))
        self.assertEqual(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id,
                              item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[u'321', u'322'], file_content={'some': 'data'}))

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 3)  # 1 request to /verify and two requests to /details
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/322')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_details_two_ids(self, mrequest, gevent_sleep):
        """Accept two ids in /verify request. Then accept 200 response for first id, and 402 and 200
        responses for second id. Check retry_get_edr_details job"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     json={'data': [{'x_edrInternalId': '321'}, {'x_edrInternalId': '322'}]}, status_code=200)
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), json={'data': {}}, status_code=200)
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=322),
                     [{'text': '', 'status_code': 402},
                      {'json': {'data': {}}, 'status_code': 200}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)

        self.assertEqual(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id, item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[u'321', u'322'], file_content={}))

        self.assertEqual(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id,
                              item_id=award_id,
                              code='123', item_name='awards', edr_ids=[u'322'],
                              file_content={}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 4)  # processed 4 requests
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/322')
        self.assertEqual(mrequest.request_history[3].url, u'127.0.0.1:80/api/1.0/details/322')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_job_get_edr_id_dead(self, mrequest, gevent_sleep):
        """Accept dict instead of list in first response to /verify endpoint. Check that worker get up"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     [{'json': {'data': {'x_edrInternalId': '321'}}, 'status_code': 200},  # data contains dict, instead of list
                      {'json': {'data': [{'x_edrInternalId': '321'}]}, 'status_code': 200}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), json={'data': {}}, status_code=200)
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)

        self.assertEqual(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id, item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[u'321'], file_content={}))

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 3)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/321')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_job_retry_get_edr_id_dead(self, mrequest, gevent_sleep):
        """Accept dict instead of list in first response to /verify endpoint. Check that worker get up"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     [{'json': {'errors': [{'description': ''}]}, 'status_code': 403},
                      {'json': {'data': {'x_edrInternalId': '321'}}, 'status_code': 200},  # data contains dict, instead of list
                      {'json': {'data': [{'x_edrInternalId': '321'}]}, 'status_code': 200}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), json={'data': {}}, status_code=200)
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)

        self.assertEqual(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id, item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[u'321'], file_content={}))

        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 4)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[3].url, u'127.0.0.1:80/api/1.0/details/321')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_job_get_edr_details_dead(self, mrequest, gevent_sleep):
        """Accept list instead of dict in response to /details/{id} endpoint. Check that worker get up"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url), json={'data': [{'x_edrInternalId': '321'}]}, status_code=200)
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321),
                     [{'json': [], 'status_code': 200},  # list instead of dict in data
                      {'json': {'data': {}}, 'status_code': 200}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)

        self.assertEqual(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id, item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[u'321'], file_content={}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 3)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/321')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_job_retry_get_edr_details_dead(self, mrequest, gevent_sleep):
        """Accept list instead of dict in response to /details/{id} endpoint. Check that worker get up"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url), json={'data': [{'x_edrInternalId': '321'}]}, status_code=200)
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321),
                     [{'text': '', 'status_code': 402},
                      {'json': [], 'status_code': 200},  # list instead of dict in data
                      {'json': {'data': {}}, 'status_code': 200}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)

        self.assertEqual(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id, item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[u'321'], file_content={}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 4)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/details/321')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_details(self, mrequest, gevent_sleep):
        """Accept 6 times errors (403 and 402 status codes) in response while requesting /details"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url), json={'data': [{'x_edrInternalId': '321'}]}, status_code=200)
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321),
                     [{'json': '', 'status_code': 403},
                      {'json': '', 'status_code': 402},
                      {'json': '', 'status_code': 402},
                      {'json': '', 'status_code': 402},
                      {'json': '', 'status_code': 402},
                      {'json': '', 'status_code': 402},
                      {'json': {'data': {}}, 'status_code': 200}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)

        self.assertEqual(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id, item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[u'321'], file_content={}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 8)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/321')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_5_times_get_edr_id(self, mrequest, gevent_sleep):
        """Accept 6 times errors (403 and 402 status codes) in response while requesting /verify"""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     [{'json': {'errors': [{'description': ''}]}, 'status_code': 403},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403},
                      {'json': {'errors': [{'description': ''}]}, 'status_code': 403},
                      {'json': {'data': [{'x_edrInternalId': '321'}]}, 'status_code': 200}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), json={'data': {}}, status_code=200)
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)
        self.assertEqual(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id, item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[u'321'], file_content={}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 8)  # processing 8 requests
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')  # check first url
        self.assertEqual(mrequest.request_history[6].url, u'127.0.0.1:80/api/1.0/verify?id=123')  # check 7th url
        self.assertEqual(mrequest.request_history[7].url, u'127.0.0.1:80/api/1.0/details/321')  # check 8th url

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_timeout(self, mrequest, gevent_sleep):
        """Raise ReadTimeout while requesting /verify, then accept 200 status code. Raise ReadTimeout while requesting
        /details/{id}, then accept 200 status code."""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url),
                     [{'exc': requests.exceptions.ReadTimeout}, {'json': {'data': [{'x_edrInternalId': 321}]}, 'status_code': 200}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321),
                     [{'exc': requests.exceptions.ReadTimeout}, {'json': {'data': {'id': 321}}, 'status_code': 200}])
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(tender_id, award_id, '123', "awards", None, None))
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)
        self.assertEqual(upload_to_doc_service_queue.get(),
                         Data(tender_id=tender_id, item_id=award_id,
                              code='123', item_name='awards',
                              edr_ids=[321], file_content={u'id': 321}))
        worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
        self.assertEqual(mrequest.call_count, 4)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[1].url, u'127.0.0.1:80/api/1.0/verify?id=123')
        self.assertEqual(mrequest.request_history[2].url, u'127.0.0.1:80/api/1.0/details/321')
        self.assertEqual(mrequest.request_history[3].url, u'127.0.0.1:80/api/1.0/details/321')


    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_identifier_id_type(self, mrequest, gevent_sleep):
        """Create filter_tenders and edr_handler workers. Test when identifier.id is type int (not str)."""
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex

        #  create queues
        filtered_tender_ids_queue = Queue(10)
        edrpou_codes_queue = Queue(10)
        edr_ids_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        filtered_tender_ids_queue.put(tender_id)

        # create workers and responses
        client = MagicMock()
        client.get_tender.side_effect = [
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': {'status': "active.pre-qualification",
                               'id': tender_id,
                               'procurementMethodType': 'aboveThresholdEU',
                               'awards': [{'id': award_id,
                                           'status': 'pending',
                                           'suppliers': [{'identifier': {
                                             'scheme': 'UA-EDR',
                                             'id': 14360570}  # int instead of str type
                                         }]}, ]}}), ]
        proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.get("{url}".format(url=proxy_client.verify_url), [{'json': {'data': [{'x_edrInternalId': 321}]}, 'status_code': 200}])
        mrequest.get("{url}/{id}".format(url=proxy_client.details_url, id=321), [{'json': {'data': {'id': 321}}, 'status_code': 200}])
        filter_tenders_worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, {})
        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue)

        self.assertEqual(upload_to_doc_service_queue.get(), Data(tender_id=tender_id, item_id=award_id, code='14360570',
                                                                 item_name='awards', edr_ids=[321], file_content={u'id': 321}))
        worker.shutdown()
        filter_tenders_worker.shutdown()
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(filtered_tender_ids_queue.qsize(), 0)
        self.assertEqual(edr_ids_queue.qsize(), 0)
