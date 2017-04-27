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

from openprocurement.bot.identification.client import DocServiceClient
from openprocurement.bot.identification.databridge.upload_file import UploadFile
from openprocurement.bot.identification.databridge.utils import Data, generate_doc_id
from openprocurement.bot.identification.tests.utils import custom_sleep, generate_answers


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
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', None, {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        self.assertItemsEqual(processing_items.keys(), [key])
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        sleep(4)
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
                                         'title': 'edr_request.yaml'}},
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
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', None, {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        self.assertItemsEqual(processing_items.keys(), [key])
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        sleep(7)
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
        client._create_tender_resource_item.side_effect = [Unauthorized(http_code=403),
                                                           Unauthorized(http_code=403),
                                                           Unauthorized(http_code=403),
                                                           Unauthorized(http_code=403),
                                                           Unauthorized(http_code=403),
                                                           Exception(),
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
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', None, {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        self.assertItemsEqual(processing_items.keys(), [key])
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        sleep(60)
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(mrequest.call_count, 1)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertEqual(processing_items, {})  # test that item removed from processing_items
        self.assertEqual(client._create_tender_resource_item.call_count, 7)  # check upload to tender

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_request_failed(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.post('{url}'.format(url=doc_service_client.url),
                      json={'data': {'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                                    'format': 'application/yaml',
                                    'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                                    'title': 'edr_request.yaml'}},
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
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', None, {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        sleep(10)
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
    def test_request_failed_in_retry(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', user='', password='')
        mrequest.post('{url}'.format(url=doc_service_client.url),
                      json={'data': {'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                                    'format': 'application/yaml',
                                    'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                                    'title': 'edr_request.yaml'}},
                      status_code=200)
        client = MagicMock()
        client._create_tender_resource_item.side_effect = [ResourceError(http_code=425),
                                                           ResourceError(http_code=422),
                                                           ResourceError(http_code=422),
                                                           ResourceError(http_code=422),
                                                           ResourceError(http_code=422),
                                                           ResourceError(http_code=422)]
        tender_id = uuid.uuid4().hex
        award_id = uuid.uuid4().hex
        document_id = generate_doc_id()
        key = '{}_{}'.format(tender_id, award_id)
        processing_items = {key: 1}
        upload_to_doc_service_queue = Queue(10)
        upload_to_tender_queue = Queue(10)
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', None, {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items, doc_service_client)
        sleep(60)
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(mrequest.call_count, 1)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertIsNotNone(mrequest.request_history[0].headers['X-Client-Request-ID'])
        self.assertEqual(processing_items, {})
        self.assertEqual(client._create_tender_resource_item.call_count, 6)  # check that processed just 1 request

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
                                    'title': 'edr_request.yaml'}},
                       'status_code': 200},
                       {'json': {'data': {
                           'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                           'format': 'application/yaml',
                           'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                           'title': 'edr_request.yaml'}},
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
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', None, {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        upload_to_doc_service_queue.put(Data(tender_id, award_id, '123', 'awards', None, {'meta': {'id': document_id}, 'test_data': 'test_data'}))
        worker = UploadFile.spawn(client, upload_to_doc_service_queue, upload_to_tender_queue, processing_items,
                                  doc_service_client)
        sleep(10)
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
                          'title': 'edr_request.yaml'}},
                        'status_code': 200},
                       {'json': {'data': {
                           'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                           'format': 'application/yaml',
                           'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                           'title': 'edr_request.yaml'}},
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
        upload_to_doc_service_queue.get.side_effect = generate_answers(
            answers=[LoopExit(), Data(tender_id, award_id, '123', 'awards', None, {'meta': {'id': document_id}, 'test_data': 'test_data'}),
                     Data(tender_id, award_id, '123', 'awards', None, {'meta': {'id': document_id}, 'test_data': 'test_data'})],
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
        upload_to_tender_queue.get.side_effect = generate_answers(
            answers=[LoopExit(),
                     Data(tender_id=tender_id,
                          item_id=award_id,
                          code='123', item_name='awards', edr_ids=None,
                          file_content={
                              'meta': {'id': document_id},
                              u'url': u'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                              u'format': u'application/yaml',
                              u'hash': u'md5:9a0364b9e99bb480dd25e1f0284c8555',
                              u'title': u'edr_request.yaml'}),
                     Data(tender_id=tender_id,
                          item_id=award_id,
                          code='123', item_name='awards', edr_ids=None,
                          file_content={
                              'meta': {'id': document_id},
                              u'url': u'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                              u'format': u'application/yaml',
                              u'hash': u'md5:9a0364b9e99bb480dd25e1f0284c8555',
                              u'title': u'edr_request.yaml'})],
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
        worker.retry_upload_to_tender_queue.get.side_effect = generate_answers(
            answers=[LoopExit(),
                     Data(tender_id=tender_id,
                          item_id=award_id,
                          code='123', item_name='awards', edr_ids=None,
                          file_content={
                              'meta': {'id': document_id},
                              u'url': u'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                              u'format': u'application/yaml',
                              u'hash': u'md5:9a0364b9e99bb480dd25e1f0284c8555',
                              u'title': u'edr_request.yaml'}),
                     Data(tender_id=tender_id,
                          item_id=award_id,
                          code='123', item_name='awards', edr_ids=None,
                          file_content={
                              'meta': {'id': document_id},
                              u'url': u'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                              u'format': u'application/yaml',
                              u'hash': u'md5:9a0364b9e99bb480dd25e1f0284c8555',
                              u'title': u'edr_request.yaml'})],
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
                          'title': 'edr_request.yaml'}},
                          'status_code': 200},
                          {'json': {'data': {
                              'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                              'format': 'application/yaml',
                              'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                              'title': 'edr_request.yaml'}},
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
        worker.retry_upload_to_doc_service_queue.get.side_effect = generate_answers(
            answers=[LoopExit(),
                     Data(tender_id, award_id, '123', 'awards', None, {'meta': {'id': document_id}, 'test_data': 'test_data'}),
                     Data(tender_id, award_id, '123', 'awards', None, {'meta': {'id': document_id}, 'test_data': 'test_data'})],
            default=LoopExit())
        sleep(10)
        worker.shutdown()
        self.assertEqual(upload_to_tender_queue.qsize(), 0, 'Queue should be empty')
        self.assertEqual(processing_items, {})
        self.assertIsNotNone(client.request_history[0].headers['X-Client-Request-ID'])
        self.assertIsNotNone(client.request_history[1].headers['X-Client-Request-ID'])
        self.assertEqual(client._create_tender_resource_item.call_count, 2)  # check that processed just 1 request