# -*- coding: utf-8 -*-
import uuid
import unittest
import datetime
import requests_mock
from gevent.queue import Queue
from time import sleep
from mock import patch, MagicMock
from restkit.errors import Unauthorized

from openprocurement.integrations.edr.client import DocServiceClient
from openprocurement.integrations.edr.databridge.upload_file import UploadFile
from openprocurement.integrations.edr.databridge.utils import Data
from openprocurement.integrations.edr.databridge.tests.utils import custom_sleep


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
                                    'title': 'edr_request.yaml'}},
                      status_code=200)
        client = MagicMock()
        client._create_tender_resource_item.side_effect = [{'data': {'id': uuid.uuid4().hex,
                                                                     'documentOf': 'tender',
                                                                     'documentType': 'registerExtract',
                                                                     'url': 'url'}}]
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        processing_items = {award_id: tender_id}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', None, {'test_data': 'test_data'}))
        self.assertItemsEqual(processing_items.keys(), [award_id])
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        sleep(4)
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(mrequest.call_count, 1)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
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
                                         'title': 'edr_request.yaml'}},
                       'status_code': 200}])
        client = MagicMock()
        client._create_tender_resource_item.side_effect = [{'data': {'id': uuid.uuid4().hex,
                                                                     'documentOf': 'tender',
                                                                     'documentType': 'registerExtract',
                                                                     'url': 'url'}}]
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        processing_items = {award_id: tender_id}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', None, {'test_data': 'test_data'}))
        self.assertItemsEqual(processing_items.keys(), [award_id])
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        sleep(7)
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(mrequest.call_count, 7)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertItemsEqual(processing_items.keys(), [])  # test that item removed from processing_items
        self.assertEqual(client._create_tender_resource_item.call_count, 1)  # check upload to tender

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_retry_upload_to_tender(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.post('{url}'.format(url=doc_service_client.url),
                      json={'data': {'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                                    'format': 'application/yaml',
                                    'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                                    'title': 'edr_request.yaml'}},
                      status_code=200)
        client = MagicMock()
        client._create_tender_resource_item.side_effect = [Unauthorized(),
                                                           Unauthorized(),
                                                           Unauthorized(),
                                                           Unauthorized(),
                                                           Unauthorized(),
                                                           Unauthorized(),
                                                           {'data': {'id': uuid.uuid4().hex,
                                                                     'documentOf': 'tender',
                                                                     'documentType': 'registerExtract',
                                                                     'url': 'url'}}]
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        processing_items = {award_id: tender_id}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', None, {'test_data': 'test_data'}))
        self.assertItemsEqual(processing_items.keys(), [award_id])
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        sleep(60)
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(mrequest.call_count, 1)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertEqual(processing_items, {})  # test that item removed from processing_items
        self.assertEqual(client._create_tender_resource_item.call_count, 7)  # check upload to tender

