# coding=utf-8

from gevent import monkey
from gevent.pywsgi import WSGIServer
from redis import StrictRedis

monkey.patch_all()

import uuid
import unittest
import subprocess

from simplejson import dumps
from time import sleep
from gevent.queue import Queue
from mock import patch
from bottle import response, request, Bottle

from openprocurement.bot.identification.tests.base import config
from openprocurement.bot.identification.databridge.constants import author, version, file_name
from openprocurement.bot.identification.databridge.caching import Db
from openprocurement.bot.identification.databridge.sleep_change_value import APIRateController
from openprocurement.bot.identification.databridge.process_tracker import ProcessTracker
from openprocurement.bot.identification.databridge.data import Data
from openprocurement.bot.identification.databridge.bridge import EdrDataBridge
from openprocurement.bot.identification.tests.utils import custom_sleep, generate_request_id

CODES = ('14360570', '0013823', '23494714')
qualification_ids = [uuid.uuid4().hex for i in range(5)]
award_ids = [uuid.uuid4().hex for i in range(5)]
request_ids = [generate_request_id() for i in range(2)]
bid_ids = [uuid.uuid4().hex for _ in range(5)]

s = 0


def setup_routing(app, func, path='/api/2.3/spore', method='GET'):
    global s
    s = 0
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
    response.content_type = 'application/json'
    response.headers.update({'X-Request-ID': request_ids[0]})
    global s
    if s == 0:
        s -= 1
        return get_tenders_response_sux()
    else:
        return get_empty_response()


def get_tenders_response_sux():
    return dumps({'prev_page': {'offset': '123'},
                  'next_page': {'offset': '1234'},
                  'data': [{'status': "active.pre-qualification",
                            "id": '123',
                            'procurementMethodType': 'aboveThresholdEU'}]})


def get_empty_response():
    return dumps({'prev_page': {'offset': '1234'},
                  'next_page': {'offset': '12345'},
                  'data': []})


def get_tender_response():
    response.status = 200
    response.content_type = 'application/json'
    response.headers.update({'X-Request-ID': request_ids[0]})
    return dumps({'prev_page': {'offset': '123'},
                  'next_page': {'offset': '1234'},
                  'data': {'status': "active.pre-qualification",
                           'id': '123',
                           'procurementMethodType': 'aboveThresholdEU',
                           'bids': [bids(2, CODES[2])],
                           'qualifications': [qualifications('pending', 2, 2)]}})


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


class EndToEndTest(unittest.TestCase):
    __test__ = True

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
        cls.redis_process = subprocess.Popen(
            ['redis-server', '--port', str(config['main']['cache_port']), '--logfile /dev/null'])
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
        del cls.api_server_bottle
        del cls.proxy_server_bottle
        del cls.doc_server_bottle

    def tearDown(self):
        del self.worker
        self.redis.flushall()

    def setUp(self):
        self.filtered_tender_ids_queue = Queue(10)
        self.edrpou_codes_queue = Queue(10)
        self.process_tracker = ProcessTracker(Db(config))
        self.tender_id = uuid.uuid4().hex
        self.sleep_change_value = APIRateController()
        self.worker = EdrDataBridge(config)

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

    def sleep_until_done(self, worker, func):
        while func(worker):
            sleep(0.1)

    def is_working_filter(self, worker):
        return worker.filtered_tender_ids_queue.qsize() or worker.edrpou_codes_queue.qsize() == 0

    def is_working_all(self, worker):
        return (worker.filtered_tender_ids_queue.qsize() or worker.edrpou_codes_queue.qsize()
                or worker.upload_to_tender_queue.qsize() or worker.upload_to_doc_service_queue.qsize())

    @patch('gevent.sleep')
    def test_scanner_and_filter(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.worker = EdrDataBridge(config)
        setup_routing(self.api_server_bottle, get_tenders_response, path='/api/2.3/tenders')
        setup_routing(self.api_server_bottle, get_tender_response, path='/api/2.3/tenders/123')
        self.worker.scanner()
        self.worker.filter_tender()
        data = Data('123', qualification_ids[2], CODES[2], "qualifications",
                    {'meta': {'sourceRequests': [request_ids[0]]}})
        self.sleep_until_done(self.worker, self.is_working_filter)
        self.assertEqual(self.worker.edrpou_codes_queue.qsize(), 1)
        self.check_data_objects(self.worker.edrpou_codes_queue.get(), data)
        self.assertEqual(self.worker.filtered_tender_ids_queue.qsize(), 0)

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
        self.sleep_until_done(self.worker, self.is_working_all)
        data = Data('123', qualification_ids[2], CODES[2], "qualifications",
                    {'meta': {'sourceRequests': [request_ids[0], request_ids[0]]}})
        self.check_data_objects(self.worker.upload_to_doc_service_queue.get(), data)
        self.assertEqual(self.worker.edrpou_codes_queue.qsize(), 0)
        self.assertEqual(self.worker.filtered_tender_ids_queue.qsize(), 0)

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
        self.sleep_until_done(self.worker, self.is_working_all)
        data = Data('123', qualification_ids[2], CODES[2], "qualifications",
                    {'meta': {}, 'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                     'format': 'application/yaml',
                     'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                     'title': file_name})
        self.assertEqual(self.worker.edrpou_codes_queue.qsize(), 0)
        self.assertEqual(self.worker.upload_to_doc_service_queue.qsize(), 0)
        self.check_data_objects(self.worker.upload_to_tender_queue.get(), data)
        self.assertEqual(self.worker.filtered_tender_ids_queue.qsize(), 0)
