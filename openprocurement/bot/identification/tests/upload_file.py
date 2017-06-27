# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import uuid
import unittest
import datetime
import requests_mock
from gevent.queue import Queue
from gevent.hub import LoopExit
from time import sleep
from mock import patch, MagicMock
from restkit.errors import Unauthorized
from restkit import ResourceError
from gevent.pywsgi import WSGIServer
from bottle import Bottle, response
from simplejson import dumps

from openprocurement.bot.identification.client import DocServiceClient
from openprocurement.bot.identification.databridge.upload_file import UploadFile
from openprocurement.bot.identification.databridge.utils import Data, generate_doc_id
from openprocurement.bot.identification.tests.utils import custom_sleep, generate_answers
from openprocurement.bot.identification.databridge.constants import file_name
from openprocurement.bot.identification.databridge.bridge import TendersClientSync

SERVER_RESPONSE_FLAG = 0
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
                           'documentType': 'registerExtract',
                           'url': 'url'}})


def generate_response():
    global SERVER_RESPONSE_FLAG
    if SERVER_RESPONSE_FLAG == 0:
        SERVER_RESPONSE_FLAG = 1
        return response_412()
    return response_get_tender()


class TestUploadFileWorker(unittest.TestCase):

    def test_init(self):
        worker = UploadFile.spawn(None, None, None, None, None)
        self.assertGreater(datetime.datetime.now().isoformat(),
                           worker.start_time.isoformat())

        self.assertEqual(worker.client, None)
        self.assertEqual(worker.upload_to_doc_service_queue, None)
        self.assertEqual(worker.upload_to_tender_queue, None)
        self.assertEqual(worker.processing_items, None)
        self.assertEqual(worker.doc_service_client, None)
        self.assertEqual(worker.delay, 15)
        self.assertEqual(worker.exit, False)

        worker.shutdown()
        self.assertEqual(worker.exit, True)
        del worker

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_successful_upload(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.post('{url}'.format(url=doc_service_client.url),
                      json={'data': {'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                                    'format': 'application/yaml',
                                    'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                                    'title': file_name}},
                      status_code=200)
        client = MagicMock()
        client._create_tender_resource_item.side_effect = [{'data': {'id': uuid.uuid4().hex,
                                                                     'documentOf': 'tender',
                                                                     'documentType': 'registerExtract',
                                                                     'url': 'url'}}]
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        self.assertItemsEqual(processing_items.keys(), [key])
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        while (upload_to_doc_service_queue.qsize() or upload_to_tender_queue.qsize() or
               worker.retry_upload_to_doc_service_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()):
            sleep(1)  # sleep while at least one queue is not empty
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(mrequest.call_count, 1)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertItemsEqual(processing_items.keys(), [])  # test that item removed from processing_items
        self.assertEqual(client._create_tender_resource_item.call_count, 1)  # check upload to tender

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_doc_service(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.post('{url}'.format(url=doc_service_client.url),
                      [{'text': '', 'status_code': 401},
                       {'text': '', 'status_code': 401},
                       {'text': '', 'status_code': 401},
                       {'text': '', 'status_code': 401},
                       {'text': '', 'status_code': 401},
                       {'text': '', 'status_code': 401},
                      {'json': {'data': {'url': 'test url',
                                         'format': 'application/yaml',
                                         'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                                         'title': file_name}},
                       'status_code': 200}])
        client = MagicMock()
        client._create_tender_resource_item.side_effect = [{'data': {'id': uuid.uuid4().hex,
                                                                     'documentOf': 'tender',
                                                                     'documentType': 'registerExtract',
                                                                     'url': 'url'}}]
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        self.assertItemsEqual(processing_items.keys(), [key])
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        while (upload_to_doc_service_queue.qsize() or upload_to_tender_queue.qsize() or
               worker.retry_upload_to_doc_service_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()):
            sleep(1)  # sleep while at least one queue is not empty
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(mrequest.call_count, 7)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertItemsEqual(processing_items.keys(), [])  # test that item removed from processing_items
        self.assertEqual(client._create_tender_resource_item.call_count, 1)  # check upload to tender

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_upload_to_tender_429(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        upload_to_tender_queue.put(Data(tender_id, award_id, '123', 'awards',
                                        {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        client = MagicMock()
        client._create_tender_resource_item = MagicMock(side_effect=[ResourceError(http_code=429), ResourceError(http_code=429), ResourceError(http_code=403)])
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        worker.client = client
        while (upload_to_doc_service_queue.qsize() or upload_to_tender_queue.qsize() or
                   worker.retry_upload_to_doc_service_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()):
            sleep(1)  # sleep while at least one queue is not empty
        worker.shutdown()
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(worker.sleep_change_value, 1)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_upload_to_tender_exception(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        upload_to_tender_queue.put(Data(tender_id, award_id, '123', 'awards',
                                        {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        client = MagicMock()
        client._create_tender_resource_item = MagicMock(side_effect=[Exception()])
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items,
                                  doc_service_client)
        worker.client = client
        worker.client_upload_to_tender = MagicMock(side_effect=ResourceError(http_code=403))
        while (upload_to_doc_service_queue.qsize() or upload_to_tender_queue.qsize() or
                worker.retry_upload_to_doc_service_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()):
            sleep(1)  # sleep while at least one queue is not empty
        worker.shutdown()
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(worker.sleep_change_value, 0)

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_upload_to_tender(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.post('{url}'.format(url=doc_service_client.url),
                      json={'data': {'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                                    'format': 'application/yaml',
                                    'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                                    'title': file_name}},
                      status_code=200)
        client = MagicMock()
        client._create_tender_resource_item.side_effect = [Unauthorized(http_code=401),
                                                           Unauthorized(http_code=403),
                                                           Unauthorized(http_code=429),
                                                           {'data': {'id': uuid.uuid4().hex,
                                                                     'documentOf': 'tender',
                                                                     'documentType': 'registerExtract',
                                                                     'url': 'url'}}]
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        self.assertItemsEqual(processing_items.keys(), [key])
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        while (upload_to_doc_service_queue.qsize() or upload_to_tender_queue.qsize() or
               worker.retry_upload_to_doc_service_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()):
            sleep(1)  # sleep while at least one queue is not empty
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(mrequest.call_count, 1)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertEqual(processing_items, {})  # test that item removed from processing_items
        self.assertEqual(client._create_tender_resource_item.call_count, 4)  # check upload to tender

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_upload_to_tender_422(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        client = MagicMock()
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items,
                                  doc_service_client)
        worker.client_upload_to_tender = MagicMock(side_effect=ResourceError(http_code=422))
        worker.retry_upload_to_tender_queue = Queue(10)
        worker.retry_upload_to_tender_queue.put(Data(tender_id, award_id, '123',
                                                     'awards', {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        while (upload_to_doc_service_queue.qsize() or upload_to_tender_queue.qsize() or
                worker.retry_upload_to_doc_service_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()):
            sleep(1)  # sleep while at least one queue is not empty
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_upload_to_tender_429(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        client = MagicMock()
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items,
                                  doc_service_client)
        client.client_upload_to_tender = MagicMock(side_effect=[ResourceError(http_code=429), ResourceError(http_code=403)])
        worker.client = client
        worker.retry_upload_to_tender_queue = Queue(10)
        worker.retry_upload_to_tender_queue.put(Data(tender_id, award_id, '123', 'awards',
                                                     {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        while (upload_to_doc_service_queue.qsize() or upload_to_tender_queue.qsize() or
                   worker.retry_upload_to_doc_service_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()):
            sleep(1)  # sleep while at least one queue is not empty
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_upload_to_tender_exception(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        client = MagicMock()
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items,
                                  doc_service_client)
        worker.client_upload_to_tender = MagicMock(side_effect=[Exception(), ResourceError(http_code=403)])
        worker.retry_upload_to_tender_queue.put(Data(tender_id, award_id, '123', 'awards',
                                                     {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        while (upload_to_doc_service_queue.qsize() or upload_to_tender_queue.qsize() or
                   worker.retry_upload_to_doc_service_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()):
            sleep(1)  # sleep while at least one queue is not empty
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_request_failed(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.post('{url}'.format(url=doc_service_client.url),
                      json={'data': {'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                                    'format': 'application/yaml',
                                    'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                                    'title': file_name}},
                      status_code=200)
        client = MagicMock()
        client._create_tender_resource_item.side_effect = ResourceError(http_code=422)
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        while (upload_to_doc_service_queue.qsize() or upload_to_tender_queue.qsize() or
               worker.retry_upload_to_doc_service_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()):
            sleep(1)  # sleep while at least one queue is not empty
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(mrequest.call_count, 1)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertEqual(processing_items, {})
        self.assertEqual(client._create_tender_resource_item.call_count, 1)  # check that processed just 1 request

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_request_failed_item_status_change(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.post('{url}'.format(url=doc_service_client.url),
                      json={'data': {'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                                    'format': 'application/yaml',
                                    'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                                    'title': file_name}},
                      status_code=200)
        client = MagicMock()
        client._create_tender_resource_item.side_effect = [ResourceError(http_code=403),
                                                           ResourceError(http_code=403)]
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        qualification_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        keys = ['{}_{}'.format(tender_id, award_id), '{}_{}'.format(tender_id, qualification_id)]
        processing_items = {keys[0]: 1, keys[1]: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        upload_to_doc_service_queue.put(Data(tender_id, qualification_id, '123', 'qualifications', {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        while (upload_to_doc_service_queue.qsize() or upload_to_tender_queue.qsize() or
               worker.retry_upload_to_doc_service_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()):
            sleep(1)  # sleep while at least one queue is not empty
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(mrequest.call_count, 2)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertEqual(processing_items, {})
        self.assertEqual(client._create_tender_resource_item.call_count, 2)  # check that processed just 1 request

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_request_failed_in_retry(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        client = MagicMock()
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        qualification_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        keys = ['{}_{}'.format(tender_id, award_id)]
        processing_items = {keys[0]: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)

        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items,
                                  doc_service_client, 3, 1.5)
        worker.client_upload_to_tender = MagicMock()
        worker.client_upload_to_tender.side_effect = [ResourceError(http_code=429),
                                                      ResourceError(http_code=429),
                                                      ResourceError(http_code=429),
                                                      ResourceError(http_code=429),
                                                      ResourceError(http_code=429),
                                                      ResourceError(http_code=403)]
        worker.retry_upload_to_tender_queue.put(
            Data(tender_id, award_id, '123', 'awards', {'meta': {'id': document_id}, 'test_data': 'test_data'}))

        while (upload_to_doc_service_queue.qsize() or upload_to_tender_queue.qsize() or
               worker.retry_upload_to_doc_service_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()):
            sleep(1)  # sleep while at least one queue is not empty
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(worker.retry_upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(worker.sleep_change_value, 13.5)
        self.assertEqual(processing_items, {})
        self.assertEqual(worker.client_upload_to_tender.call_count, 6)  # check that processed just 1 request

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_request_failed_in_retry_item_status(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.post('{url}'.format(url=doc_service_client.url),
                      json={'data': {'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                                     'format': 'application/yaml',
                                     'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                                     'title': file_name}},
                      status_code=200)
        client = MagicMock()
        client._create_tender_resource_item.side_effect = [ResourceError(http_code=429),
                                                           ResourceError(http_code=403),
                                                           ResourceError(http_code=403),
                                                           ResourceError(http_code=403),
                                                           ResourceError(http_code=403)
                                                           ]
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        qualification_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        keys = ['{}_{}'.format(tender_id, award_id)]
        processing_items = {keys[0]: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        worker.retry_upload_to_tender_queue.put(Data(tender_id, award_id, '123', 'awards', {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        while (upload_to_doc_service_queue.qsize() or upload_to_tender_queue.qsize() or
                   worker.retry_upload_to_doc_service_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()):
            sleep(1)  # sleep while at least one queue is not empty
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(processing_items, {})
        self.assertEqual(client._create_tender_resource_item.call_count, 5)  # check that processed just 1 request

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_processing_items(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        client = MagicMock()
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.post('{url}'.format(url=doc_service_client.url),
                      [{'json': {'data': {'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                                    'format': 'application/yaml',
                                    'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                                    'title': file_name}},
                       'status_code': 200},
                       {'json': {'data': {
                           'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                           'format': 'application/yaml',
                           'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                           'title': file_name}},
                        'status_code': 200}])
        client._create_tender_resource_item.side_effect = [{'data': {'id': uuid.uuid4().hex,
                                                                     'documentOf': 'tender',
                                                                     'documentType': 'registerExtract',
                                                                     'url': 'url'}},
                                                           {'data': {'id': uuid.uuid4().hex,
                                                                     'documentOf': 'tender',
                                                                     'documentType': 'registerExtract',
                                                                     'url': 'url'}}]
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 2}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items,
                                  doc_service_client)
        while (upload_to_doc_service_queue.qsize() or upload_to_tender_queue.qsize() or
               worker.retry_upload_to_doc_service_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()):
            sleep(1)  # sleep while at least one queue is not empty
        worker.shutdown()
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(processing_items, {})
        self.assertEqual(client._create_tender_resource_item.call_count, 2)  # check that processed just 1 request

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_upload_to_doc_service_queue_loop_exit(self, mrequest, gevent_sleep):
        """ Test LoopExit for upload_to_doc_service_queue """
        gevent_sleep.side_effect = custom_sleep
        client = MagicMock()
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.post('{url}'.format(url=doc_service_client.url),
                      [{'json': {'data': {
                          'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                          'format': 'application/yaml',
                          'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                          'title': file_name}},
                        'status_code': 200},
                       {'json': {'data': {
                           'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                           'format': 'application/yaml',
                           'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                           'title': file_name}},
                           'status_code': 200}])
        client._create_tender_resource_item.side_effect = [
            {'data': {'id': uuid.uuid4().hex,
                      'documentOf': 'tender',
                      'documentType': 'registerExtract',
                      'url': 'url'}},
            {'data': {'id': uuid.uuid4().hex,
                      'documentOf': 'tender',
                      'documentType': 'registerExtract',
                      'url': 'url'}}]
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 2}
        upload_to_doc_service_queue = MagicMock()
        upload_to_tender_queue = Queue(10)
        upload_to_doc_service_queue.peek.side_effect = generate_answers(
            answers=[LoopExit(),
                     Data(tender_id, award_id, '123', 'awards', {'meta': {'id': document_id}, 'test_data': 'test_data'}),
                     Data(tender_id, award_id, '123', 'awards', {'meta': {'id': document_id}, 'test_data': 'test_data'})],
            default=LoopExit())
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        sleep(10)
        worker.shutdown()
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertIsNotNone(mrequest.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(processing_items, {})
        self.assertEqual(client._create_tender_resource_item.call_count, 2)  # check that processed just 1 request

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_upload_to_tender_queue_loop_exit(self, mrequest, gevent_sleep):
        """ Test LoopExit for upload_to_tender_queue """
        gevent_sleep.side_effect = custom_sleep
        client = MagicMock()
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        client._create_tender_resource_item.side_effect = [
            {'data': {'id': uuid.uuid4().hex,
                      'documentOf': 'tender',
                      'documentType': 'registerExtract',
                      'url': 'url'}},
            {'data': {'id': uuid.uuid4().hex,
                      'documentOf': 'tender',
                      'documentType': 'registerExtract',
                      'url': 'url'}}]
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 2}
        upload_to_doc_service_queue = Queue(1)
        upload_to_tender_queue = MagicMock()
        upload_to_tender_queue.peek.side_effect = generate_answers(
            answers=[LoopExit(),
                     Data(tender_id=tender_id,
                          item_id=award_id,
                          code='123', item_name='awards',
                          file_content={
                              u'meta': {u'id': document_id},
                              u'url': u'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                              u'format': u'application/yaml',
                              u'hash': u'md5:9a0364b9e99bb480dd25e1f0284c8555',
                              u'title': file_name}),
                     Data(tender_id=tender_id,
                          item_id=award_id,
                          code='123', item_name='awards',
                          file_content={
                              u'meta': {u'id': document_id},
                              u'url': u'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                              u'format': u'application/yaml',
                              u'hash': u'md5:9a0364b9e99bb480dd25e1f0284c8555',
                              u'title': file_name})],
            default=LoopExit())
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        sleep(10)
        worker.shutdown()
        self.assertEqual(processing_items, {})
        self.assertIsNotNone(client.request_history[0].headers['X-Client-Request-ID'])
        self.assertIsNotNone(client.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(client._create_tender_resource_item.call_count, 2)  # check that processed just 1 request

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_upload_to_tender_queue_loop_exit(self, mrequest, gevent_sleep):
        """ Test LoopExit for retry_upload_to_tender_queue """
        gevent_sleep.side_effect = custom_sleep
        client = MagicMock()
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80',
                                              user='', password='')
        client._create_tender_resource_item.side_effect = [
            {'data': {'id': uuid.uuid4().hex,
                      'documentOf': 'tender',
                      'documentType': 'registerExtract',
                      'url': 'url'}},
            {'data': {'id': uuid.uuid4().hex,
                      'documentOf': 'tender',
                      'documentType': 'registerExtract',
                      'url': 'url'}}]
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 2}
        upload_to_doc_service_queue = Queue(1)
        upload_to_tender_queue = Queue(1)
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        worker.retry_upload_to_tender_queue = MagicMock()
        worker.retry_upload_to_tender_queue.peek.side_effect = generate_answers(
            answers=[LoopExit(),
                     Data(tender_id=tender_id,
                          item_id=award_id,
                          code='123', item_name='awards',
                          file_content={
                              u'meta': {u'id': document_id},
                              u'url': u'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                              u'format': u'application/yaml',
                              u'hash': u'md5:9a0364b9e99bb480dd25e1f0284c8555',
                              u'title': file_name}),
                     Data(tender_id=tender_id,
                          item_id=award_id,
                          code='123', item_name='awards',
                          file_content={
                              u'meta': {u'id': document_id},
                              u'url': u'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                              u'format': u'application/yaml',
                              u'hash': u'md5:9a0364b9e99bb480dd25e1f0284c8555',
                              u'title': file_name})],
            default=LoopExit())
        sleep(10)
        worker.shutdown()
        self.assertEqual(processing_items, {})
        self.assertIsNotNone(client.request_history[0].headers['X-Client-Request-ID'])
        self.assertIsNotNone(client.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(client._create_tender_resource_item.call_count, 2)  # check that processed just 1 request

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_upload_to_doc_service_queue_loop_exit(self, mrequest, gevent_sleep):
        """ Test LoopExit for retry_upload_to_doc_service_queue """
        gevent_sleep.side_effect = custom_sleep
        client = MagicMock()
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.post('{url}'.format(url=doc_service_client.url),
                      [{'json': {'data': {
                          'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                          'format': 'application/yaml',
                          'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                          'title': file_name}},
                          'status_code': 200},
                          {'json': {'data': {
                              'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                              'format': 'application/yaml',
                              'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                              'title': file_name}},
                              'status_code': 200}])
        client._create_tender_resource_item.side_effect = [
            {'data': {'id': uuid.uuid4().hex,
                      'documentOf': 'tender',
                      'documentType': 'registerExtract',
                      'url': 'url'}},
            {'data': {'id': uuid.uuid4().hex,
                      'documentOf': 'tender',
                      'documentType': 'registerExtract',
                      'url': 'url'}}]
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 2}
        upload_to_tender_queue = Queue(1)
        upload_to_doc_service_queue = Queue(1)
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        worker.retry_upload_to_doc_service_queue = MagicMock()
        worker.retry_upload_to_doc_service_queue.peek.side_effect = generate_answers(
            answers=[LoopExit(),
                     Data(tender_id, award_id, '123', 'awards', {'meta': {'id': document_id}, 'test_data': 'test_data'}),
                     Data(tender_id, award_id, '123', 'awards', {'meta': {'id': document_id}, 'test_data': 'test_data'})],
            default=LoopExit())
        sleep(10)
        worker.shutdown()
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(processing_items, {})
        self.assertIsNotNone(client.request_history[0].headers['X-Client-Request-ID'])
        self.assertIsNotNone(client.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(client._create_tender_resource_item.call_count, 2)  # check that processed just 1 request

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_412(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.post('{url}'.format(url=doc_service_client.url),
                      json={'data': {'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                                     'format': 'application/yaml',
                                     'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                                     'title': file_name}},
                      status_code=200)
        api_server_bottle = Bottle()
        api_server = WSGIServer(('127.0.0.1', 20604), api_server_bottle, log=None)
        setup_routing(api_server_bottle, response_spore)
        setup_routing(api_server_bottle, response_412, path='/api/2.3/tenders/123456789/awards/124/documents', method='POST')
        api_server.start()
        client = TendersClientSync('', host_url='http://127.0.0.1:20604', api_version='2.3')
        setup_routing(api_server_bottle, generate_response, path='/api/2.3/tenders/123456789/awards/124/documents', method='POST')
        self.assertEqual(client.headers['Cookie'], 'SERVER_ID={}'.format(SPORE_COOKIES))  # check that response_spore set cookies
        document_id = generate_doc_id()
        key = '{}_{}'.format('123456789', '124')
        processing_items = {key: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        upload_to_doc_service_queue.put(Data('123456789', '124', '123', 'awards',
                                             {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        self.assertItemsEqual(processing_items.keys(), [key])
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        while (upload_to_doc_service_queue.qsize() or upload_to_tender_queue.qsize() or
               worker.retry_upload_to_doc_service_queue.qsize() or worker.retry_upload_to_tender_queue.qsize()):
            sleep(1)  # sleep while at least one queue is not empty
        self.assertEqual(client.headers['Cookie'], 'SERVER_ID={}'.format(COOKIES_412))  # check that response_412 change cookies
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(mrequest.call_count, 1)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertItemsEqual(processing_items.keys(), [])  # test that item removed from processing_items