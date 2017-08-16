# coding=utf-8

from gevent import monkey, subprocess

monkey.patch_all()

import os
import unittest
import uuid

from simplejson import dumps

from time import sleep
from gevent.queue import Queue
from mock import patch, MagicMock
from gevent.pywsgi import WSGIServer
from munch import munchify
from redis import StrictRedis
from requests import RequestException
from bottle import Bottle, response, request
from restkit import RequestError

from openprocurement.bot.identification.databridge.caching import Db
from openprocurement.bot.identification.databridge.constants import author, version, file_name
from openprocurement.bot.identification.databridge.filter_tender import FilterTenders
from openprocurement.bot.identification.databridge.sleep_change_value import APIRateController
from openprocurement.bot.identification.databridge.utils import ProcessTracker, Data
from openprocurement_client.client import TendersClientSync, TendersClient
from openprocurement.bot.identification.databridge.bridge import EdrDataBridge
from openprocurement.bot.identification.client import DocServiceClient, ProxyClient
from openprocurement.bot.identification.tests.utils import custom_sleep, ResponseMock, generate_request_id

config = {
    'main':
        {
            'tenders_api_server': 'http://127.0.0.1:20604',
            'tenders_api_version': '2.3',
            'public_tenders_api_server': 'http://127.0.0.1:20605',
            'doc_service_server': 'http://127.0.0.1',
            'doc_service_port': 20606,
            'doc_service_user': 'broker',
            'doc_service_password': 'broker_pass',
            'proxy_server': 'http://127.0.0.1',
            'proxy_port': 20607,
            'proxy_user': 'robot',
            'proxy_password': 'robot',
            'proxy_version': 1.0,
            'buffers_size': 450,
            'full_stack_sync_delay': 15,
            'empty_stack_sync_delay': 101,
            'on_error_sleep_delay': 5,
            'api_token': "api_token",
            'cache_db_name': 0,
            'cache_host': '127.0.0.1',
            'cache_port': '16379',
            'time_to_live': 1800,
            'delay': 1,
            'time_to_live_negative': 120
        }
}

CODES = ('14360570', '0013823', '23494714')
qualification_ids = [uuid.uuid4().hex for i in range(5)]
award_ids = [uuid.uuid4().hex for i in range(5)]
request_ids = [generate_request_id() for i in range(2)]
bid_ids = [uuid.uuid4().hex for _ in range(5)]



def setup_routing(app, func, path='/api/2.3/spore', method='GET'):
    app.route(path, method, func)


def response_spore():
    response.set_cookie("SERVER_ID", ("a7afc9b1fc79e640f2487ba48243ca071c07a823d27"
                                      "8cf9b7adf0fae467a524747e3c6c6973262130fac2b"
                                      "96a11693fa8bd38623e4daee121f60b4301aef012c"))
    return response


def doc_response():
    return response


def awards(counter_id, counter_bid_id, status, sup_id):
    return {'id': award_ids[counter_id], 'bid_id': bid_ids[counter_bid_id], 'status': status,
            'suppliers': [{'identifier': {'scheme': 'UA-EDR', 'id': sup_id}}]}


def bids(counter_id, edr_id):
    return {'id': bid_ids[counter_id], 'tenderers': [{'identifier': {'scheme': 'UA-EDR', 'id': edr_id}}]}


def qualifications(status, counter_qual_id, counter_bid_id):
    return {'status': status, 'id': qualification_ids[counter_qual_id], 'bidID': bid_ids[counter_bid_id]}


def proxy_response():
    if request.headers.get("sandbox-mode") != "True":  # Imitation of health comparison
        response.status = 400
    return response


def get_tenders_response():
    return munchify({'prev_page': {'offset': '123'},
                     'next_page': {'offset': '1234'},
                     'data': [{'status': "active.pre-qualification",
                               "id": '123',
                               'procurementMethodType': 'aboveThresholdEU'}]})


def get_tender_response():
    response.status = 200
    response.content_type = 'application/json'
    response.headers.update({'X-Request-ID': request_ids[0]})
    return dumps({'prev_page': {'offset': '123'},
                  'next_page': {'offset': '1234'},
                  'data': {'status': "active.pre-qualification",
                           'id': '123',
                           'procurementMethodType': 'aboveThresholdEU',
                           'bids': [bids(0, CODES[0]),
                                    bids(1, CODES[1]),
                                    bids(2, CODES[2]),
                                    bids(3, CODES[2]),
                                    {'id': bid_ids[4],
                                     'tenderers': [{'identifier': {
                                         'scheme': 'UA-ED',
                                         'id': CODES[2]}}]}],
                           'qualifications': [
                               qualifications('pending', 2, 2),
                           ]}})


def get_proxy_response():
    response.status = 200
    response.content_type = 'application/json'
    response.headers.update({'X-Request-ID': request_ids[0]})
    return {'data': [{}],
            "meta": {"sourceDate": "2017-04-25T11:56:36+00:00", "id": "{}".format(generate_request_id()),
                     "version": version, 'author': author,
                     'detailsSourceDate': ["2017-04-25T11:56:36+00:00"]}}


def get_doc_service_response():
    response.status = 200
    return {'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
            'format': 'application/yaml',
            'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
            'title': file_name}


class BaseServersTest(unittest.TestCase):
    """Api server to test openprocurement.integrations.edr.databridge.bridge """

    relative_to = os.path.dirname(__file__)  # crafty line

    @classmethod
    def setUpClass(cls):
        cls.api_server_bottle = Bottle()
        cls.proxy_server_bottle = Bottle()
        cls.doc_server_bottle = Bottle()
        cls.api_server = WSGIServer(('127.0.0.1', 20604), cls.api_server_bottle, log=None)
        setup_routing(cls.api_server_bottle, response_spore)
        cls.public_api_server = WSGIServer(('127.0.0.1', 20605), cls.api_server_bottle, log=None)
        cls.doc_server = WSGIServer(('127.0.0.1', 20606), cls.doc_server_bottle, log=None)
        setup_routing(cls.doc_server_bottle, doc_response, path='/')
        cls.proxy_server = WSGIServer(('127.0.0.1', 20607), cls.proxy_server_bottle, log=None)
        setup_routing(cls.proxy_server_bottle, proxy_response, path='/api/1.0/health')
        cls.redis_process = subprocess.Popen(['redis-server', '--port', str(config['main']['cache_port'])])
        sleep(0.1)
        cls.redis = StrictRedis(port=str(config['main']['cache_port']))

        # start servers
        cls.api_server.start()
        cls.public_api_server.start()
        cls.doc_server.start()
        cls.proxy_server.start()

    @classmethod
    def tearDownClass(cls):
        cls.api_server.close()
        cls.public_api_server.close()
        cls.doc_server.close()
        cls.proxy_server.close()
        cls.redis_process.terminate()
        cls.redis_process.wait()

    def tearDown(self):
        # del self.worker
        self.redis.flushall()



class EndToEndTest(BaseServersTest):
    def check_data_objects(self, obj, example):
        self.assertEqual(obj.tender_id, example.tender_id)
        self.assertEqual(obj.item_id, example.item_id)
        self.assertEqual(obj.code, example.code)
        self.assertEqual(obj.item_name, example.item_name)
        self.assertIsNotNone(obj.file_content['meta']['id'])
        if obj.file_content['meta'].get('author'):
            self.assertEqual(obj.file_content['meta']['author'], author)
        if obj.file_content['meta'].get('sourceRequests'):
            self.assertEqual(obj.file_content['meta']['sourceRequests'], example.file_content['meta']['sourceRequests'])

    def setUp(self):
        self.filtered_tender_ids_queue = Queue(10)
        self.edrpou_codes_queue = Queue(10)
        self.process_tracker = ProcessTracker()
        self.tender_id = uuid.uuid4().hex
        self.sleep_change_value = APIRateController()

    def test_init(self):
        self.worker = EdrDataBridge(config)
        self.assertEqual(self.worker.delay, 1)
        self.assertEqual(self.worker.sleep_change_value.time_between_requests, 0)
        self.assertTrue(isinstance(self.worker.tenders_sync_client, TendersClientSync))
        self.assertTrue(isinstance(self.worker.client, TendersClient))
        self.assertTrue(isinstance(self.worker.proxyClient, ProxyClient))
        self.assertTrue(isinstance(self.worker.doc_service_client, DocServiceClient))
        self.assertFalse(self.worker.initialization_event.is_set())
        self.assertEqual(self.worker.process_tracker.processing_items, {})
        self.assertEqual(self.worker.db._backend, "redis")
        self.assertEqual(self.worker.db._db_name, 0)
        self.assertEqual(self.worker.db._port, "16379")
        self.assertEqual(self.worker.db._host, "127.0.0.1")

    @patch('gevent.sleep')
    def test_scanner_and_filter(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.worker = EdrDataBridge(config)
        setup_routing(self.api_server_bottle, get_tender_response, path='/api/2.3/tenders/123')
        setup_routing(self.api_server_bottle, get_tenders_response, path='/api/2.3/tenders')
        self.worker.scanner()
        self.worker.filter_tender()
        sleep(3)
        data = Data('123', qualification_ids[2], CODES[2], "qualifications",
                    {'meta': {'sourceRequests': [request_ids[0]]}})
        self.assertEqual(self.worker.edrpou_codes_queue.qsize(), 1)
        self.check_data_objects(self.worker.edrpou_codes_queue.get(), data)

    @patch('gevent.sleep')
    def test_scanner_to_edr_handler(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.worker = EdrDataBridge(config)
        setup_routing(self.api_server_bottle, get_tender_response, path='/api/2.3/tenders/123')
        setup_routing(self.api_server_bottle, get_tenders_response, path='/api/2.3/tenders')
        setup_routing(self.proxy_server_bottle, get_proxy_response, path='/api/1.0/verify')
        self.worker.scanner()
        self.worker.filter_tender()
        self.worker.edr_handler()
        sleep(3)
        data = Data('123', qualification_ids[2], CODES[2], "qualifications",
                    {'meta': {'sourceRequests': [request_ids[0], request_ids[0]]}})
        self.check_data_objects(self.worker.upload_to_doc_service_queue.get(), data)
        self.assertEqual(self.worker.edrpou_codes_queue.qsize(), 0)

    @patch('gevent.sleep')
    def test_scanner_to_upload_to_doc_service(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.worker = EdrDataBridge(config)
        setup_routing(self.api_server_bottle, get_tender_response, path='/api/2.3/tenders/123')
        setup_routing(self.api_server_bottle, get_tenders_response, path='/api/2.3/tenders')
        setup_routing(self.proxy_server_bottle, get_proxy_response, path='/api/1.0/verify')
        setup_routing(self.doc_server_bottle, get_doc_service_response, path='/upload', method='POST')
        self.worker.scanner()
        self.worker.filter_tender()
        self.worker.edr_handler()
        self.worker.upload_file_to_doc_service()
        sleep(3)
        data = Data('123', qualification_ids[2], CODES[2], "qualifications",
                    {'meta': {}, 'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                     'format': 'application/yaml',
                     'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                     'title': file_name})
        self.assertEqual(self.worker.edrpou_codes_queue.qsize(), 0)
        self.assertEqual(self.worker.upload_to_doc_service_queue.qsize(), 0)
        self.check_data_objects(self.worker.upload_to_tender_queue.get(), data)
