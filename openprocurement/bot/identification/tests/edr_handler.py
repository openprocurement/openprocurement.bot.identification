# -*- coding: utf-8 -*-
from requests import Response

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
from openprocurement.bot.identification.tests.utils import custom_sleep, generate_answers, generate_request_id, \
    ResponseMock
from openprocurement.bot.identification.client import ProxyClient
from openprocurement.bot.identification.databridge.constants import version, author
from openprocurement.bot.identification.databridge.sleep_change_value import APIRateController


def get_random_edr_ids(count=1):
    return [str(random.randrange(10000000, 99999999)) for _ in range(count)]


class TestEdrHandlerWorker(unittest.TestCase):
    def setUp(self):
        self.source_date = ["2017-04-25T11:56:36+00:00"]
        self.gen_req_id = [generate_request_id() for _ in xrange(10)]
        self.edrpou_codes_queue = Queue(10)
        self.edr_ids_queue = Queue(10)
        self.upload_to_doc_service_queue = Queue(10)
        self.filtered_tender_ids_queue = Queue(10)
        self.tender_id = uuid.uuid4().hex
        self.document_id = generate_doc_id()
        self.award_id = uuid.uuid4().hex
        self.edr_req_ids = [generate_request_id() for _ in xrange(10)]
        self.proxy_client = ProxyClient(host='127.0.0.1', port='80', user='', password='')
        self.uri = "{uri}".format(uri=self.proxy_client.verify_url)
        self.url = "{url}".format(url=self.proxy_client.verify_url)
        self.local_edr_ids = get_random_edr_ids(2)
        self.edr_ids = get_random_edr_ids(1)[0]
        self.processing_items = {}
        self.worker = EdrHandler.spawn(self.proxy_client, self.edrpou_codes_queue,
                                       self.upload_to_doc_service_queue, self.processing_items, MagicMock())
        self.sleep_change_value = APIRateController()

    def meta(self):
        return {'meta': {'id': self.document_id, 'author': author, 'sourceRequests': [
            'req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}

    def tearDown(self):
        self.worker.shutdown()
        self.assertEqual(self.edrpou_codes_queue.qsize(), 0)
        self.assertEqual(self.edr_ids_queue.qsize(), 0)
        del self.worker

    @staticmethod
    def stat_c(st_code, ret_aft, err_desc, x_req_id):
        if ret_aft == 0:
            return {'json': {'errors': [{'description': err_desc}]}, 'status_code': st_code,
                    'headers': {'X-Request-ID': x_req_id}}
        else:
            return {'json': {'errors': [{'description': err_desc}]}, 'status_code': st_code,
                    'headers': {'Retry-After': ret_aft, 'X-Request-ID': x_req_id}}

    @staticmethod
    def stat_200(data_info, det_source_date, x_req_id):
        return {'json': {'data': data_info, "meta": {"detailsSourceDate": det_source_date}},
                'status_code': 200, 'headers': {'X-Request-ID': x_req_id}}

    @staticmethod
    def file_con(data_info, doc_id, suf_1, suf_2, source_req):
        if suf_1 == 1 and suf_2 == 1:
            return {'data': data_info,
                    "meta": {"sourceDate": "2017-04-25T11:56:36+00:00", "id": "{}".format(doc_id),
                             "version": version, 'author': author,
                             'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220'] + source_req}}
        else:
            return {'data': data_info,
                    "meta": {"sourceDate": "2017-04-25T11:56:36+00:00", "id": "{}.{}.{}".format(doc_id, suf_1, suf_2),
                             "version": version, 'author': author,
                             'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220'] + source_req}}

    @staticmethod
    def urls(inf):
        return u'127.0.0.1:80/api/1.0/{}'.format(inf)

    def url_id(self, u_id):
        return "{url}/{id}".format(url=self.proxy_client.verify_url, id=u_id)

    def test_init(self):
        worker = EdrHandler.spawn(None, None, None, None, None)
        self.assertGreater(datetime.datetime.now().isoformat(),
                           worker.start_time.isoformat())
        self.assertEqual(worker.proxyClient, None)
        self.assertEqual(worker.edrpou_codes_queue, None)
        self.assertEqual(worker.upload_to_doc_service_queue, None)
        self.assertEqual(worker.delay, 15)
        self.assertEqual(worker.exit, False)
        worker.shutdown()
        self.assertEqual(worker.exit, True)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_proxy_client(self, mrequest, gevent_sleep):
        """ Test that proxy return json with id """
        gevent_sleep.side_effect = custom_sleep
        mrequest.get(self.url, [self.stat_200([{}], self.source_date, self.edr_req_ids[0]),
                                self.stat_200([{}], self.source_date, self.edr_req_ids[1])])
        expected_result = []
        for i in range(2):
            self.edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids, "awards",
                                             self.file_con({}, self.document_id, 1, 1, [])))  # data
            expected_result.append(
                Data(self.tender_id, self.award_id, self.edr_ids, "awards",
                     self.file_con({}, self.document_id, 1, 1, [self.edr_req_ids[i]])))
        for result in expected_result:
            self.assertEquals(self.upload_to_doc_service_queue.get(), result)
        self.assertEqual(mrequest.request_history[0].url, self.urls('verify?id={}').format(self.edr_ids))
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.call_count, 2)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_proxy_client_429(self, mrequest, gevent_sleep):
        """Accept 429 status code in first request with header 'Retry-After'"""
        gevent_sleep.side_effect = custom_sleep
        mrequest.get(self.uri, [self.stat_c(429, '1', '', self.edr_req_ids[0]),
                                self.stat_200([{}], self.source_date, self.edr_req_ids[0]),
                                self.stat_c(429, '1', '', self.edr_req_ids[1]),
                                self.stat_200([{}], self.source_date, self.edr_req_ids[1])])
        expected_result = []
        for i in range(2):
            self.edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))
            expected_result.append(
                Data(self.tender_id, self.award_id, self.edr_ids, "awards",
                     self.file_con({}, self.document_id, 1, 1, [self.edr_req_ids[i], self.edr_req_ids[i]])))  # result
        for result in expected_result:
            self.assertEquals(self.upload_to_doc_service_queue.get(), result)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_proxy_client_402(self, mrequest, gevent_sleep):
        """First request returns Edr API returns to proxy 402 status code with messages."""
        gevent_sleep.side_effect = custom_sleep
        edr_details_req_id = self.gen_req_id[0:2]
        mrequest.get(self.uri, [self.stat_c(403, 0, [{'message': 'Payment required.', 'code': 5}], self.edr_req_ids[0]),
                                # pay for me
                                self.stat_200([{}], self.source_date,
                                              self.edr_req_ids[0]),
                                self.stat_c(403, 0, [{'message': 'Payment required.', 'code': 5}], self.edr_req_ids[1]),
                                # pay for me
                                self.stat_200([{}], self.source_date,
                                              self.edr_req_ids[1])])
        expected_result = []
        for i in xrange(2):
            mrequest.get(self.url_id(self.local_edr_ids[i]),
                         [self.stat_200([{}], self.source_date, edr_details_req_id[i])])
            self.edrpou_codes_queue.put(
                Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))
            expected_result.append(
                Data(self.tender_id, self.award_id, self.edr_ids, "awards",
                     self.file_con({}, self.document_id, 1, 1, [self.edr_req_ids[i], self.edr_req_ids[i]])))
        for result in expected_result:
            self.assertEquals(self.upload_to_doc_service_queue.get(), result)
        self.assertIsNotNone(mrequest.request_history[3].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_id(self, mrequest, gevent_sleep):
        """First and second response returns 403 status code. Tests retry for get_edr_id worker"""
        gevent_sleep.side_effect = custom_sleep
        edr_details_req_ids = self.gen_req_id[0]
        local_edr_ids = get_random_edr_ids(1)
        mrequest.get(self.uri, [self.stat_c(403, 0, '', edr_details_req_ids),
                                self.stat_c(403, 0, '', edr_details_req_ids),
                                self.stat_200([{}], self.source_date, edr_details_req_ids)])
        mrequest.get(self.url_id(local_edr_ids[0]), [self.stat_200([{}], self.source_date, edr_details_req_ids)])
        expected_result = []
        for i in range(1):
            self.edrpou_codes_queue.put(
                Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))
            expected_result.append(Data(self.tender_id, self.award_id, self.edr_ids, "awards",
                                        self.file_con({}, self.document_id, 1, 1,
                                                      [edr_details_req_ids, edr_details_req_ids])))
        self.assertEquals(self.upload_to_doc_service_queue.get(), expected_result[0])
        self.assertIsNotNone(mrequest.request_history[2].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_get_edr_data_empty_response(self, mrequest, gevent_sleep):
        """Accept response with 404 status code and error message 'EDRPOU not found'. Check that tender_data
        is in self.upload_to_doc_service_queue."""
        gevent_sleep.side_effect = custom_sleep
        edr_req_id = self.gen_req_id[0]
        mrequest.get(self.url, json={'errors': [{'description':
                                                     [{"error": {"errorDetails": "Couldn't find this code in EDR.",
                                                                 "code": "notFound"},
                                                       "meta": {"detailsSourceDate": self.source_date}}]}]},
                     status_code=404, headers={'X-Request-ID': edr_req_id})
        self.edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))
        self.assertEquals(self.upload_to_doc_service_queue.get(),
                          Data(self.tender_id, self.award_id, self.edr_ids, 'awards',
                               {"error": {"errorDetails": "Couldn't find this code in EDR.",
                                          "code": "notFound"},
                                "meta": {"detailsSourceDate": self.source_date,
                                         "id": self.document_id, "version": version,
                                         'author': author,
                                         "sourceRequests": ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                            edr_req_id]}}))
        # check that data not in self.edr_ids_queue
        self.assertEqual(mrequest.call_count, 1)  # Requests must call proxy once
        self.assertEqual(mrequest.request_history[0].url, self.urls('verify?id={}'.format(self.edr_ids)))

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_get_edr_data_two_ids(self, mrequest, gevent_sleep):
        """Accept wrong format in first request, put to retry, check the results"""
        gevent_sleep.side_effect = custom_sleep
        mrequest.get(self.uri,
                     [self.stat_200([{"test": 1}, {"test": 2}], [self.source_date], self.edr_req_ids[0]),
                      self.stat_200([{"test": 1}, {"test": 2}],
                                    ["2017-04-25T11:56:36+00:00", "2017-04-25T11:56:36+00:00"], self.edr_req_ids[1])])
        edrpou_codes_queue = Queue(10)
        check_queue = Queue(10)
        expected_result = []
        edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))  # data
        expected_result.append(Data(self.tender_id, self.award_id, self.edr_ids, "awards",
                                    self.file_con({"test": 1}, self.document_id, 2, 1,
                                                  [self.edr_req_ids[0], self.edr_req_ids[1]])))  # result
        expected_result.append(Data(self.tender_id, self.award_id, self.edr_ids, "awards",
                                    self.file_con({"test": 2}, self.document_id, 2, 2,
                                                  [self.edr_req_ids[0], self.edr_req_ids[1]])))  # result
        self.worker = EdrHandler.spawn(self.proxy_client, edrpou_codes_queue, check_queue, MagicMock(), MagicMock())
        for result in expected_result:
            self.assertEquals(check_queue.get(), result)
        self.assertEqual(self.worker.retry_edrpou_codes_queue.qsize(), 0, 'Queue must be empty')
        self.assertEqual(check_queue.qsize(), 0, 'Queue must be empty')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_data_two_ids(self, mrequest, gevent_sleep):
        """Accept 429 status code in first request with header 'Retry-After'"""
        gevent_sleep.side_effect = custom_sleep
        mrequest.get(self.uri,
                     [self.stat_c(403, '10', '', self.edr_req_ids[0]),
                      self.stat_200([{"test": 1}, {"test": 2}],
                                    ["2017-04-25T11:56:36+00:00", "2017-04-25T11:56:36+00:00"], self.edr_req_ids[1])])
        edrpou_codes_queue = Queue(10)
        check_queue = Queue(10)
        expected_result = []
        edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))  # data
        expected_result.append(Data(self.tender_id, self.award_id, self.edr_ids, "awards",
                                    self.file_con({"test": 1}, self.document_id, 2, 1,
                                                  [self.edr_req_ids[0], self.edr_req_ids[1]])))  # result
        expected_result.append(Data(self.tender_id, self.award_id, self.edr_ids, "awards",
                                    self.file_con({"test": 2}, self.document_id, 2, 2,
                                                  [self.edr_req_ids[0], self.edr_req_ids[1]])))
        self.worker = EdrHandler.spawn(self.proxy_client, edrpou_codes_queue, check_queue, MagicMock(), MagicMock())
        for result in expected_result:
            self.assertEquals(check_queue.get(), result)
        self.assertEqual(self.worker.retry_edrpou_codes_queue.qsize(), 0, 'Queue must be empty')
        self.assertEqual(check_queue.qsize(), 0, 'Queue must be empty')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_data_empty_response(self, mrequest, gevent_sleep):
        """Accept 5 times response with status code 403 and error, then accept response with status code 404 and
        message 'EDRPOU not found'"""
        gevent_sleep.side_effect = custom_sleep
        edr_req_id = self.gen_req_id[0]
        mrequest.get(self.uri, [self.stat_c(403, 0, '', edr_req_id), self.stat_c(403, 0, '', edr_req_id),
                                self.stat_c(403, 0, '', edr_req_id), self.stat_c(403, 0, '', edr_req_id),
                                self.stat_c(403, 0, '', edr_req_id),
                                {'json': {
                                    'errors': [
                                        {'description': [{"error": {"errorDetails": "Couldn't find this code in EDR.",
                                                                    "code": "notFound"},
                                                          "meta": {"detailsSourceDate": self.source_date}}]}]},
                                    'status_code': 404, 'headers': {'X-Request-ID': edr_req_id}}])
        self.edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))
        self.assertEquals(self.upload_to_doc_service_queue.get(),
                          Data(self.tender_id, self.award_id, self.edr_ids, 'awards',
                               {"error": {"errorDetails": "Couldn't find this code in EDR.",
                                          "code": "notFound"},
                                "meta": {"detailsSourceDate": self.source_date,
                                         'id': self.document_id,
                                         "version": version, 'author': author,
                                         'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                            edr_req_id]}}))
        # check that data not in self.edr_ids_queue
        self.assertEqual(mrequest.call_count, 6)
        self.assertEqual(mrequest.request_history[0].url, self.urls('verify?id={}'.format(self.edr_ids)))
        self.assertEqual(mrequest.request_history[5].url, self.urls('verify?id={}'.format(self.edr_ids)))

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_data_mock_403(self, mrequest, gevent_sleep):
        """Accept 429 status code in first request with header 'Retry-After'"""
        gevent_sleep.side_effect = custom_sleep
        edr_req_id = self.gen_req_id[0]
        edrpou_codes_queue = Queue(10)
        check_queue = Queue(10)
        expected_result = []
        self.worker = EdrHandler.spawn(self.proxy_client, edrpou_codes_queue, check_queue, MagicMock(), MagicMock())
        self.worker.retry_edrpou_codes_queue.put(
            Data(self.tender_id, self.award_id, '123', "awards", self.meta()))  # data
        expected_result.append(Data(self.tender_id, self.award_id, '123', "awards",
                                    file_content={"error": {"errorDetails": "Couldn't find this code in EDR.",
                                                            "code": "notFound"},
                                                  "meta": {"detailsSourceDate": self.source_date,
                                                           'id': self.document_id, "version": version, 'author': author,
                                                           'sourceRequests': [
                                                               'req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))  # result
        self.worker.get_edr_data_request = MagicMock(side_effect=[
            RetryException("test", MagicMock(status_code=403)),
            RetryException("test", MagicMock(status_code=404, headers={'X-Request-ID': edr_req_id},
                                             json=MagicMock(return_value=
                                             {"errors":
                                                 [{"description":
                                                     [{"error": {
                                                         "errorDetails": "Couldn't find this code in EDR.",
                                                         "code": u"notFound"},
                                                         "meta": {
                                                             "detailsSourceDate": self.source_date,
                                                             'id': self.document_id, "version": version,
                                                             'author': author
                                                         }}]}]})))])
        for result in expected_result:
            self.assertEquals(check_queue.get(), result)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_data_mock_404(self, mrequest, gevent_sleep):
        """Accept 429 status code in first request with header 'Retry-After'"""
        gevent_sleep.side_effect = custom_sleep
        edr_req_id = self.gen_req_id[0]
        edrpou_codes_queue = Queue(10)
        check_queue = Queue(10)
        expected_result = []
        self.worker = EdrHandler.spawn(self.proxy_client, edrpou_codes_queue, check_queue, MagicMock(), MagicMock())
        self.worker.retry_edrpou_codes_queue.put(
            Data(self.tender_id, self.award_id, '123', "awards", self.meta()))  # data
        expected_result.append(Data(self.tender_id, self.award_id, '123', "awards",
                                    file_content={"error": {"errorDetails": "Couldn't find this code in EDR.",
                                                            "code": "notFound"},
                                                  "meta": {"detailsSourceDate": self.source_date,
                                                           'id': self.document_id, "version": version, 'author': author,
                                                           'sourceRequests': [
                                                               'req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))  # result
        self.worker.get_edr_data_request = MagicMock(side_effect=[
            RetryException("test", MagicMock(status_code=404, headers={'X-Request-ID': edr_req_id},
                                             json=MagicMock(return_value=
                                             {"errors":
                                                 [{"description":
                                                     [{"error": {
                                                         "errorDetails": "Couldn't find this code in EDR.",
                                                         "code": u"notFound"},
                                                         "meta": {
                                                             "detailsSourceDate": self.source_date,
                                                             'id': self.document_id, "version": version,
                                                             'author': author}}]}]})))])
        for result in expected_result:
            self.assertEquals(check_queue.get(), result)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_get_edr_data_mock_exception(self, mrequest, gevent_sleep):
        """Accept 429 status code in first request with header 'Retry-After'"""
        gevent_sleep.side_effect = custom_sleep
        edr_req_id = self.gen_req_id[0]
        edrpou_codes_queue = Queue(10)
        check_queue = Queue(10)
        expected_result = []
        self.worker = EdrHandler.spawn(self.proxy_client, edrpou_codes_queue, check_queue, MagicMock(), MagicMock())
        self.worker.retry_edrpou_codes_queue.put(
            Data(self.tender_id, self.award_id, '123', "awards", self.meta()))  # data
        expected_result.append(Data(self.tender_id, self.award_id, '123', "awards",
                                    file_content={"error": {"errorDetails": "Couldn't find this code in EDR.",
                                                            "code": "notFound"},
                                                  "meta": {"detailsSourceDate": self.source_date,
                                                           'id': self.document_id, "version": version, 'author': author,
                                                           'sourceRequests': [
                                                               'req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))  # result
        self.worker.get_edr_data_request = MagicMock(side_effect=[
            Exception(),
            RetryException("test", MagicMock(status_code=404, headers={'X-Request-ID': edr_req_id},
                                             json=MagicMock(return_value=
                                             {"errors":
                                                 [{"description":
                                                     [{"error": {
                                                         "errorDetails": "Couldn't find this code in EDR.",
                                                         "code": u"notFound"},
                                                         "meta": {
                                                             "detailsSourceDate": self.source_date,
                                                             'id': self.document_id, "version": version,
                                                             'author': author}}]}]})))])
        for result in expected_result:
            self.assertEquals(check_queue.get(), result)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_job_get_edr_data_dead(self, mrequest, gevent_sleep):
        """Recieve 404 and not valid data (worker dies). Check that worker get up"""
        gevent_sleep.side_effect = custom_sleep
        edr_details_req_id = self.gen_req_id[0]
        edr_req_id = self.gen_req_id[0:2]
        mrequest.get(self.url, [{'json': {'data': [{}]},
                                 'headers': {'X-Request-ID': edr_req_id[0]}},  # data contains dict, instead of list
                                self.stat_200([{}], self.source_date, edr_req_id[1])])
        mrequest.get(self.url_id(321), [self.stat_200([{}], self.source_date, edr_details_req_id)])
        self.edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))
        self.assertEquals(self.upload_to_doc_service_queue.get(),
                          Data(self.tender_id, self.award_id, self.edr_ids, 'awards',
                               self.file_con({}, self.document_id, 1, 1, [edr_req_id[0], edr_req_id[1]])))
        self.assertEqual(mrequest.call_count, 2)
        self.assertEqual(mrequest.request_history[0].url, self.urls('verify?id={}'.format(self.edr_ids)))
        self.assertEqual(mrequest.request_history[1].url, self.urls('verify?id={}'.format(self.edr_ids)))
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_job_retry_get_edr_data_dead(self, mrequest, gevent_sleep):
        """Accept dict instead of list in first response to /verify endpoint. Check that worker get up"""
        gevent_sleep.side_effect = custom_sleep
        edr_req_id = self.gen_req_id[0:3]
        mrequest.get(self.url, [self.stat_c(403, 0, '', edr_req_id[0]),
                                {'json': {'data': [{}]}, 'status_code': 200,
                                 'headers': {'X-Request-ID': edr_req_id[1]}},
                                self.stat_200([{}], self.source_date, edr_req_id[2])])
        edrpou_codes_queue = Queue(10)
        edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))
        self.worker = EdrHandler.spawn(self.proxy_client, edrpou_codes_queue, self.upload_to_doc_service_queue,
                                       MagicMock(), MagicMock())
        self.assertEquals(self.upload_to_doc_service_queue.get(),
                          Data(self.tender_id, self.award_id, self.edr_ids, 'awards',
                               self.file_con({}, self.document_id, 1, 1, [edr_req_id[0], edr_req_id[1], edr_req_id[2]])))
        self.assertEqual(mrequest.call_count, 3)
        self.assertEqual(mrequest.request_history[0].url, self.urls('verify?id={}'.format(self.edr_ids)))
        self.assertEqual(mrequest.request_history[1].url, self.urls('verify?id={}'.format(self.edr_ids)))
        self.assertEqual(mrequest.request_history[2].url, self.urls('verify?id={}'.format(self.edr_ids)))
        self.assertIsNotNone(mrequest.request_history[2].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_5_times_get_edr_data(self, mrequest, gevent_sleep):
        """Accept 6 times errors in response while requesting /verify"""
        gevent_sleep.side_effect = custom_sleep
        edr_req_id = self.gen_req_id[0:8]
        mrequest.get(self.url,
                     [self.stat_c(403, 0, '', edr_req_id[0]),
                      self.stat_c(403, 0, '', edr_req_id[1]),
                      self.stat_c(403, 0, '', edr_req_id[2]),
                      self.stat_c(403, 0, '', edr_req_id[3]),
                      self.stat_c(403, 0, '', edr_req_id[4]),
                      self.stat_c(403, 0, '', edr_req_id[5]),
                      self.stat_200([{}], self.source_date, edr_req_id[6])])
        edrpou_codes_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))
        self.worker = EdrHandler.spawn(self.proxy_client, edrpou_codes_queue, upload_to_doc_service_queue, MagicMock(), MagicMock())
        self.assertEquals(upload_to_doc_service_queue.get(),
                          Data(self.tender_id, self.award_id, self.edr_ids, 'awards',
                               self.file_con({}, self.document_id, 1, 1, [edr_req_id[0], edr_req_id[6]])))
        self.assertEqual(mrequest.call_count, 7)  # processing 7 requests
        self.assertEqual(mrequest.request_history[0].url,
                         self.urls('verify?id={}'.format(self.edr_ids)))  # check first url
        self.assertEqual(mrequest.request_history[6].url,
                         self.urls('verify?id={}'.format(self.edr_ids)))  # check 7th url
        self.assertIsNotNone(mrequest.request_history[6].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_timeout(self, mrequest, gevent_sleep):
        """Accept 'Gateway Timeout Error'  while requesting /verify, then accept 200 status code."""
        gevent_sleep.side_effect = custom_sleep
        edr_details_req_id = [self.gen_req_id[0], self.document_id]
        edr_req_id = self.gen_req_id[0:7]
        mrequest.get(self.url, [self.stat_c(403, 0, [{u'message': u'Gateway Timeout Error'}], edr_req_id[0]),
                                self.stat_200([{}], self.source_date, edr_req_id[1])])
        mrequest.get(self.url_id(321), [self.stat_c(403, 0, [{u'message': u'Gateway Timeout Error'}],
                                                    edr_details_req_id[0]),
                                        self.stat_200([{}], self.source_date, edr_details_req_id[1])])
        self.edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))
        self.assertEquals(self.upload_to_doc_service_queue.get(),
                          Data(self.tender_id, self.award_id, self.edr_ids, 'awards',
                               self.file_con({}, self.document_id, 1, 1,
                                             [edr_req_id[0], edr_req_id[1]])))
        self.assertEqual(mrequest.call_count, 2)
        self.assertEqual(mrequest.request_history[0].url, self.urls('verify?id={}'.format(self.edr_ids)))
        self.assertEqual(mrequest.request_history[1].url, self.urls('verify?id={}'.format(self.edr_ids)))

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_identifier_id_type(self, mrequest, gevent_sleep):
        """Create filter_tenders and edr_handler workers. Test when identifier.id is type int (not str)."""
        gevent_sleep.side_effect = custom_sleep
        bid_id = uuid.uuid4().hex
        edr_req_id = self.gen_req_id[0]

        #  create queues
        filtered_tender_ids_queue = Queue(10)
        edrpou_codes_queue = Queue(10)
        upload_to_doc_service_queue = Queue(10)
        filtered_tender_ids_queue.put(self.tender_id)

        # create workers and responses
        client = MagicMock()
        client.request.return_value = ResponseMock({'X-Request-ID': 'req-db3ed1c6-9843-415f-92c9-7d4b08d39220'},
                                                   munchify({'prev_page': {'offset': '123'},
                                                             'next_page': {'offset': '1234'},
                                                             'data': {'status': "active.pre-qualification",
                                                                      'id': self.tender_id,
                                                                      'procurementMethodType': 'aboveThresholdEU',
                                                                      'awards': [{'id': self.award_id,
                                                                                  'status': 'pending',
                                                                                  'bid_id': bid_id,
                                                                                  'suppliers': [{'identifier': {
                                                                                      'scheme': 'UA-EDR',
                                                                                      'id': 14360570}
                                                                                      # int instead of str type
                                                                                  }]}, ]}}))
        mrequest.get(self.url, [self.stat_200([{}], self.source_date, edr_req_id)])
        filter_tenders_worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, {},
                                                    MagicMock(), {}, self.sleep_change_value)
        self.worker = EdrHandler.spawn(self.proxy_client, edrpou_codes_queue, upload_to_doc_service_queue,
                                       MagicMock(), MagicMock())

        obj = upload_to_doc_service_queue.get()
        self.assertEqual(obj.tender_id, self.tender_id)
        self.assertEqual(obj.item_id, self.award_id)
        self.assertEqual(obj.code, '14360570')
        self.assertEqual(obj.item_name, 'awards')
        self.assertEqual(obj.file_content['data'], {})
        self.assertEqual(obj.file_content['meta']['sourceDate'], self.source_date[0])
        self.assertIsNotNone(obj.file_content['meta']['id'])
        self.assertEqual(obj.file_content['meta']['version'], version)
        self.assertEqual(obj.file_content['meta']['author'], author)
        self.assertEqual(obj.file_content['meta']['sourceRequests'],
                         ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220', edr_req_id])
        filter_tenders_worker.shutdown()
        self.assertEqual(filtered_tender_ids_queue.qsize(), 0)
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_processing_items(self, mrequest, gevent_sleep):
        """Return list of objects from EDR. Check number of edr_ids in processing_items."""
        gevent_sleep.side_effect = custom_sleep
        qualification_id = uuid.uuid4().hex
        document_ids = [generate_doc_id(), generate_doc_id()]
        edr_req_id = self.gen_req_id[0:2]
        award_key = '{}_{}'.format(self.tender_id, self.award_id)
        qualification_key = '{}_{}'.format(self.tender_id, qualification_id)
        data_1 = Data(self.tender_id, self.award_id, '123', "awards",
                      self.file_con({}, document_ids[0], 2, 1, [edr_req_id[0]]))
        data_2 = Data(self.tender_id, self.award_id, '123', "awards",
                      self.file_con({}, document_ids[0], 2, 2, [edr_req_id[0]]))
        data_3 = Data(self.tender_id, qualification_id, '124', 'qualifications',
                      self.file_con({}, document_ids[1], 3, 1, [edr_req_id[1]]))
        data_4 = Data(self.tender_id, qualification_id, '124', 'qualifications',
                      self.file_con({}, document_ids[1], 3, 2, [edr_req_id[1]]))
        data_5 = Data(self.tender_id, qualification_id, '124', 'qualifications',
                      self.file_con({}, document_ids[1], 3, 3, [edr_req_id[1]]))
        mrequest.get(self.url, [
            self.stat_200([{}, {}], ["2017-04-25T11:56:36+00:00", "2017-04-25T11:56:36+00:00"], edr_req_id[0]),
            self.stat_200([{}, {}, {}],
                          ["2017-04-25T11:56:36+00:00", "2017-04-25T11:56:36+00:00",
                           "2017-04-25T11:56:36+00:00"], edr_req_id[1])])

        #  create queues
        edrpou_codes_queue = Queue(10)
        upload_to_doc_service = Queue(10)
        edrpou_codes_queue.put(Data(self.tender_id, self.award_id, '123', "awards",
                                    {'meta': {'id': document_ids[0], 'author': author,
                                              'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))
        edrpou_codes_queue.put(Data(self.tender_id, qualification_id, '124', 'qualifications',
                                    {'meta': {'id': document_ids[1], 'author': author,
                                              'sourceRequests': ['req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))

        self.worker = EdrHandler.spawn(self.proxy_client, edrpou_codes_queue, upload_to_doc_service,
                                       self.processing_items, MagicMock())

        for data in [data_1, data_2, data_3, data_4, data_5]:
            self.assertEquals(upload_to_doc_service.get(), data)
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertEqual(self.processing_items[award_key], 2)
        self.assertEqual(self.processing_items[qualification_key], 3)
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_wrong_ip(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        edr_req_id = self.gen_req_id[0:2]
        edr_details_req_id = self.gen_req_id[0:2]
        mrequest.get(self.url, [self.stat_c(403, 0, [{u'message': u'Forbidden'}], edr_req_id[0]),
                                self.stat_200([{}], self.source_date, edr_req_id[1])])
        mrequest.get(self.url_id(321), [self.stat_c(403, 0, [{u'message': u'Forbidden'}], edr_details_req_id[0]),
                                        self.stat_200([{}], self.source_date, edr_details_req_id[1])])
        self.edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))
        self.assertEquals(self.upload_to_doc_service_queue.get(),
                          Data(self.tender_id, self.award_id, self.edr_ids, 'awards',
                               self.file_con({}, self.document_id, 1, 1, [edr_req_id[0], edr_req_id[1]])))
        self.assertEqual(mrequest.call_count, 2)
        self.assertEqual(mrequest.request_history[0].url, self.urls('verify?id={}'.format(self.edr_ids)))
        self.assertEqual(mrequest.request_history[1].url, self.urls('verify?id={}'.format(self.edr_ids)))

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_edrpou_codes_queue_loop_exit(self, mrequest, gevent_sleep):
        """ Test LoopExit for edrpou_codes_queue """
        document_ids = [self.document_id, self.document_id]
        gevent_sleep.side_effect = custom_sleep
        edr_req_id = self.gen_req_id[0:2]
        edr_details_req_id = self.gen_req_id[0:2]
        mrequest.get(self.uri,
                     [self.stat_200([{}], self.source_date, edr_req_id[0]),
                      self.stat_200([{}], self.source_date, edr_req_id[1])])
        mrequest.get(self.url_id(self.local_edr_ids[0]),
                     [self.stat_200([{}], self.source_date, edr_details_req_id[0])])
        mrequest.get(self.url_id(self.local_edr_ids[1]),
                     [self.stat_200([{}], self.source_date, edr_details_req_id[1])])
        edrpou_codes_queue = MagicMock()
        expected_result = []
        edrpou_codes_queue_list = [LoopExit()]
        for i in range(2):
            edrpou_codes_queue_list.append(Data(self.tender_id, self.award_id, self.edr_ids, "awards",
                                                {'meta': {'id': document_ids[i], 'author': author, 'sourceRequests': [
                                                    'req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))  # data
            expected_result.append(
                Data(self.tender_id, self.award_id, self.edr_ids, "awards",
                     self.file_con({}, document_ids[i], 1, 1, [edr_req_id[i]])))  # result
        edrpou_codes_queue.peek.side_effect = generate_answers(answers=edrpou_codes_queue_list, default=LoopExit())
        self.worker.retry_edrpou_codes_queue = MagicMock()
        self.worker = EdrHandler.spawn(self.proxy_client, edrpou_codes_queue, self.upload_to_doc_service_queue,
                                       MagicMock(), MagicMock())
        for result in expected_result:
            self.assertEquals(self.upload_to_doc_service_queue.get(), result)
        self.assertEqual(mrequest.request_history[0].url, self.urls('verify?id={}').format(expected_result[0].code))
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.call_count, 2)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_edrpou_codes_queue_loop_exit(self, mrequest, gevent_sleep):
        """ Test LoopExit for retry_edrpou_codes_queue """
        document_ids = [self.document_id, self.document_id]
        gevent_sleep.side_effect = custom_sleep
        edr_req_id = self.gen_req_id[0:2]
        edr_details_req_id = self.gen_req_id[0:2]
        mrequest.get(self.uri, [self.stat_200([{}], self.source_date, edr_req_id[0]),
                                self.stat_200([{}], self.source_date, edr_req_id[1])])
        mrequest.get(self.url_id(self.local_edr_ids[0]), [self.stat_200([{}], self.source_date, edr_details_req_id[0])])
        mrequest.get(self.url_id(self.local_edr_ids[1]), [self.stat_200([{}], self.source_date, edr_details_req_id[1])])
        edrpou_codes_queue_list = [LoopExit()]
        expected_result = []
        for i in range(2):
            edrpou_codes_queue_list.append(Data(self.tender_id, self.award_id, self.edr_ids, "awards",
                                                {"meta": {"id": document_ids[i], 'author': author, 'sourceRequests': [
                                                    'req-db3ed1c6-9843-415f-92c9-7d4b08d39220']}}))  # data
            expected_result.append(
                Data(self.tender_id, self.award_id, self.edr_ids, "awards",
                     self.file_con({}, document_ids[i], 1, 1, [edr_req_id[i]])))  # result
        self.worker.retry_edrpou_codes_queue = MagicMock()
        self.worker.retry_edrpou_codes_queue.peek.side_effect = generate_answers(answers=edrpou_codes_queue_list,
                                                                                 default=LoopExit())
        for result in expected_result:
            self.assertEquals(self.upload_to_doc_service_queue.get(), result)
        self.assertEqual(mrequest.request_history[0].url, self.urls('verify?id={}').format(expected_result[0].code))
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.call_count, 2)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_exception(self, mrequest, gevent_sleep):
        """ Raise RetryException  in retry_get_edr_data"""
        gevent_sleep.side_effect = custom_sleep
        edr_req_ids = self.gen_req_id[0:2]
        retry_response = MagicMock()
        retry_response.status_code = 500
        mrequest.get(self.url,
                     [{'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids[0]}},
                      {'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids[0]}},
                      {'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids[0]}},
                      {'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids[0]}},
                      {'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids[0]}},
                      {'exc': RetryException('Retry Exception', retry_response)},
                      {'json': {'data': [{}], "meta": {"detailsSourceDate": self.source_date}},
                       'status_code': 200, 'headers': {'X-Request-ID': edr_req_ids[1]}}])
        edrpou_codes_queue = Queue(10)
        check_queue = Queue(10)
        expected_result = []
        for i in range(1):
            edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids[i], "awards", self.meta()))  # data
            expected_result.append(Data(self.tender_id, self.award_id, self.edr_ids[i], "awards",
                                        self.file_con({}, self.document_id, 1, 1,
                                                      [edr_req_ids[0], edr_req_ids[1]])))  # result
        self.worker = EdrHandler.spawn(self.proxy_client, edrpou_codes_queue, check_queue, MagicMock(), MagicMock())
        for result in expected_result:
            self.assertEquals(check_queue.get(), result)
        self.assertEqual(mrequest.request_history[0].url, self.urls('verify?id={}'.format(expected_result[0].code)))
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[6].url, self.urls('verify?id={}'.format(expected_result[0].code)))
        self.assertIsNotNone(mrequest.request_history[6].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.call_count, 7)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_exception_404(self, mrequest, gevent_sleep):
        """ Raise RetryException  in retry_get_edr_data with status_code 404"""
        gevent_sleep.side_effect = custom_sleep
        edr_req_ids = self.gen_req_id[0]
        retry_response = MagicMock()
        retry_response.status_code = 404
        retry_response.json = MagicMock()
        retry_response.json.return_value = {'errors': [
            {'description': [{'error': {"errorDetails": "Couldn't find this code in EDR.", 'code': "notFound"},
                              'meta': {"detailsSourceDate": self.source_date}}]}]}
        mrequest.get(self.url,
                     [{'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids}},
                      {'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids}},
                      {'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids}},
                      {'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids}},
                      {'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids}},
                      {'exc': RetryException('Retry Exception', retry_response)}])

        edrpou_codes_queue = Queue(10)
        check_queue = Queue(10)

        expected_result = []
        for i in range(1):
            edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids[i], "awards", self.meta()))  # data
            expected_result.append(Data(self.tender_id, self.award_id, self.edr_ids[i], "awards",
                                        {'error': {'errorDetails': "Couldn't find this code in EDR.",
                                                   'code': 'notFound'},
                                         'meta': {'detailsSourceDate': self.source_date,
                                                  'id': self.document_id, "version": version, 'author': author,
                                                  'sourceRequests': [
                                                      'req-db3ed1c6-9843-415f-92c9-7d4b08d39220',
                                                      edr_req_ids]}}))  # result

            self.worker = EdrHandler.spawn(self.proxy_client, edrpou_codes_queue, check_queue, MagicMock(), MagicMock())

        for result in expected_result:
            self.assertEquals(check_queue.get(), result)
        self.assertEqual(mrequest.request_history[0].url, self.urls('verify?id={}'.format(expected_result[0].code)))
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.call_count, 6)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_exception(self, mrequest, gevent_sleep):
        """ Raise Exception  in retry_get_edr_data"""
        gevent_sleep.side_effect = custom_sleep
        edr_req_ids = self.gen_req_id[0:2]
        mrequest.get(self.url,
                     [{'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids[0]}},
                      {'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids[0]}},
                      {'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids[0]}},
                      {'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids[0]}},
                      {'status_code': 500, 'headers': {'X-Request-ID': edr_req_ids[0]}},
                      {'exc': Exception()}, self.stat_200([{}], self.source_date, edr_req_ids[1])])

        edrpou_codes_queue = Queue(10)
        check_queue = Queue(10)

        expected_result = []
        for i in range(1):
            edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids[i], "awards", self.meta()))  # data
            expected_result.append(Data(self.tender_id, self.award_id, self.edr_ids[i], "awards",
                                        self.file_con({}, self.document_id, 1, 1,
                                                      [edr_req_ids[0], edr_req_ids[1]])))  # result
        self.worker = EdrHandler.spawn(self.proxy_client, edrpou_codes_queue, check_queue, MagicMock(), MagicMock())
        for result in expected_result:
            self.assertEquals(check_queue.get(), result)
        self.assertEqual(mrequest.request_history[0].url, self.urls('verify?id={}'.format(expected_result[0].code)))
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.request_history[6].url, self.urls('verify?id={}'.format(expected_result[0].code)))
        self.assertIsNotNone(mrequest.request_history[6].headers['X-Client-Request-ID'])
        self.assertEqual(mrequest.call_count, 7)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_value_error(self, mrequest, gevent_sleep):
        """Accept 'Gateway Timeout Error'  while requesting /verify, then accept 200 status code."""
        gevent_sleep.side_effect = custom_sleep
        edr_req_id = self.gen_req_id[0:2]
        mrequest.get(self.url,
                     [self.stat_c(403, 0, [{u'message': u'Gateway Timeout Error'}], edr_req_id[0]),
                      {"text": "resp", 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[1]}},
                      {"text": "resp", 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[1]}},
                      {"text": "resp", 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[1]}},
                      {"text": "resp", 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[1]}},
                      {"text": "resp", 'status_code': 403, 'headers': {'X-Request-ID': edr_req_id[1]}},
                      self.stat_200([{}], self.source_date, edr_req_id[1])])
        self.edrpou_codes_queue.put(Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))
        self.worker.retry_edr_ids_queue = MagicMock()
        self.assertEquals(self.upload_to_doc_service_queue.get(),
                          Data(self.tender_id, self.award_id, self.edr_ids, 'awards',
                               self.file_con({}, self.document_id, 1, 1, [edr_req_id[0], edr_req_id[1]])))
        self.assertEqual(mrequest.call_count, 7)
        self.assertEqual(mrequest.request_history[0].url, self.urls('verify?id={}'.format(self.edr_ids)))
        self.assertEqual(mrequest.request_history[1].url, self.urls('verify?id={}'.format(self.edr_ids)))
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_value_error_mock(self, mrequest, gevent_sleep):
        """Accept 'Gateway Timeout Error'  while requesting /verify, then accept 200 status code."""
        gevent_sleep.side_effect = custom_sleep
        edr_req_id = self.gen_req_id[0:2]
        self.worker.retry_edr_ids_queue = MagicMock()
        self.worker.retry_edrpou_codes_queue.put(
            Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))
        self.worker.get_edr_data_request = MagicMock(side_effect=[RetryException("Exception", Response()),
                                                                  MagicMock(headers={'X-Request-ID': edr_req_id[0]},
                                                                            status_code=200,
                                                                            json=MagicMock(return_value=munchify(
                                                                                {'data': [{}], "meta": {
                                                                                    "detailsSourceDate":
                                                                                        self.source_date}})))])
        self.assertEquals(self.upload_to_doc_service_queue.get(),
                          Data(self.tender_id, self.award_id, self.edr_ids, 'awards',
                               self.file_con({}, self.document_id, 1, 1, [edr_req_id[0]])))

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_429_mock(self, mrequest, gevent_sleep):
        """Accept 'Gateway Timeout Error'  while requesting /verify, then accept 200 status code."""
        gevent_sleep.side_effect = custom_sleep
        edr_req_id = self.gen_req_id[0:2]
        self.worker.retry_edr_ids_queue = MagicMock()
        self.worker.retry_edrpou_codes_queue.put(
            Data(self.tender_id, self.award_id, self.edr_ids, "awards", self.meta()))
        self.worker.get_edr_data_request = MagicMock(
            side_effect=[MagicMock(headers={'X-Request-ID': edr_req_id[0], 'Retry-After': '1'}, status_code=429),
                         MagicMock(headers={'X-Request-ID': edr_req_id[1]}, status_code=200,
                                   json=MagicMock(return_value=munchify(
                                       {'data': [{}], "meta": {"detailsSourceDate": self.source_date}})))])
        self.assertEquals(self.upload_to_doc_service_queue.get(),
                          Data(self.tender_id, self.award_id, self.edr_ids, 'awards',
                               self.file_con({}, self.document_id, 1, 1, [edr_req_id[0], edr_req_id[1]])))
