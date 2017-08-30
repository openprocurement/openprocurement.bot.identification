# -*- coding: utf-8 -*-
from gevent import monkey
from openprocurement.bot.identification.databridge.caching import Db

monkey.patch_all()

import uuid
import datetime

from gevent.hub import LoopExit
from gevent.queue import Queue
from mock import patch, MagicMock
from time import sleep
from munch import munchify
from restkit.errors import Unauthorized, ResourceError, RequestFailed
from bottle import response
from simplejson import dumps
from gevent import event

from base import config, BaseServersTest
from utils import custom_sleep, generate_request_id, ResponseMock
from openprocurement.bot.identification.databridge.constants import author
from openprocurement.bot.identification.databridge.filter_tender import FilterTenders
from openprocurement.bot.identification.databridge.utils import item_key
from openprocurement.bot.identification.databridge.process_tracker import ProcessTracker
from openprocurement.bot.identification.databridge.data import Data
from openprocurement.bot.identification.databridge.bridge import TendersClientSync
from openprocurement.bot.identification.databridge.sleep_change_value import APIRateController

SERVER_RESPONSE_FLAG = 0
SPORE_COOKIES = ("a7afc9b1fc79e640f2487ba48243ca071c07a823d27"
                 "8cf9b7adf0fae467a524747e3c6c6973262130fac2b"
                 "96a11693fa8bd38623e4daee121f60b4301aef012c")
COOKIES_412 = ("b7afc9b1fc79e640f2487ba48243ca071c07a823d27"
               "8cf9b7adf0fae467a524747e3c6c6973262130fac2b"
               "96a11693fa8bd38623e4daee121f60b4301aef012c")
CODES = ('14360570', '0013823', '23494714')


def setup_routing(app, func, path='/api/2.3/spore', method='GET'):
    app.route(path, method, func)


def response_spore():
    response.set_cookie("SERVER_ID", SPORE_COOKIES)
    return response


def response_412():
    response.status = 412
    response.set_cookie("SERVER_ID", COOKIES_412)
    return response


def response_get_tender():
    response.status = 200
    response.headers['X-Request-ID'] = '125'
    return dumps({'prev_page': {'offset': '123'},
                  'next_page': {'offset': '1234'},
                  'data': {'status': "active.pre-qualification",
                           'id': '123',
                           'procurementMethodType': 'aboveThresholdEU',
                           'awards': [{'id': '124',
                                       'bid_id': '111',
                                       'status': 'pending',
                                       'suppliers': [{'identifier': {
                                           'scheme': 'UA-EDR',
                                           'id': CODES[0]}}]}]}})


def generate_response():
    global SERVER_RESPONSE_FLAG
    if SERVER_RESPONSE_FLAG == 0:
        SERVER_RESPONSE_FLAG = 1
        return response_412()
    return response_get_tender()


class TestFilterWorker(BaseServersTest):
    __test__ = True

    def setUp(self):
        self.filtered_tender_ids_queue = Queue(10)
        self.edrpou_codes_queue = Queue(10)
        self.db = Db(config)
        self.process_tracker = ProcessTracker(self.db)
        self.tender_id = uuid.uuid4().hex
        self.filtered_tender_ids_queue.put(self.tender_id)
        self.sleep_change_value = APIRateController()
        self.client = MagicMock()
        self.sna = event.Event()
        self.sna.set()
        self.worker = FilterTenders.spawn(self.client, self.filtered_tender_ids_queue, self.edrpou_codes_queue,
                                          self.process_tracker, self.sna, self.sleep_change_value)
        self.bid_ids = [uuid.uuid4().hex for _ in range(5)]
        self.qualification_ids = [uuid.uuid4().hex for _ in range(5)]
        self.award_ids = [uuid.uuid4().hex for _ in range(5)]
        self.request_ids = [generate_request_id() for _ in range(2)]
        self.response = ResponseMock({'X-Request-ID': self.request_ids[0]},
                                     munchify({'prev_page': {'offset': '123'},
                                               'next_page': {'offset': '1234'},
                                               'data': {'status': "active.pre-qualification",
                                                        'id': self.tender_id,
                                                        'procurementMethodType': 'aboveThresholdEU',
                                                        'awards': [self.awards(0, 0, 'pending', CODES[0])]}}))

    def tearDown(self):
        self.worker.shutdown()
        del self.worker
        self.redis.flushall()

    def awards(self, counter_id, counter_bid_id, status, sup_id):
        return {'id': self.award_ids[counter_id], 'bid_id': self.bid_ids[counter_bid_id], 'status': status,
                'suppliers': [{'identifier': {'scheme': 'UA-EDR', 'id': sup_id}}]}

    def bids(self, counter_id, edr_id):
        return {'id': self.bid_ids[counter_id], 'tenderers': [{'identifier': {'scheme': 'UA-EDR', 'id': edr_id}}]}

    def qualifications(self, status, counter_qual_id, counter_bid_id):
        return {'status': status, 'id': self.qualification_ids[counter_qual_id], 'bidID': self.bid_ids[counter_bid_id]}

    def check_data_objects(self, obj, example):
        """Checks that two data objects are equal,
                  that Data.file_content.meta.id is not none and
                  that Data.file_content.meta.author exists and is equal to IdentificationBot
         """
        self.assertEqual(obj.tender_id, example.tender_id)
        self.assertEqual(obj.item_id, example.item_id)
        self.assertEqual(obj.code, example.code)
        self.assertEqual(obj.item_name, example.item_name)
        self.assertIsNotNone(obj.file_content['meta']['id'])
        self.assertEqual(obj.file_content['meta']['author'], author)
        self.assertEqual(obj.file_content['meta']['sourceRequests'], example.file_content['meta']['sourceRequests'])

    def test_init(self):
        worker = FilterTenders.spawn(None, None, None, None, self.sna, self.sleep_change_value)
        self.assertGreater(datetime.datetime.now().isoformat(), worker.start_time.isoformat())
        self.assertEqual(worker.tenders_sync_client, None)
        self.assertEqual(worker.filtered_tender_ids_queue, None)
        self.assertEqual(worker.edrpou_codes_queue, None)
        self.assertEqual(worker.process_tracker, None)
        self.assertEqual(worker.services_not_available, self.sna)
        self.assertEqual(worker.sleep_change_value.time_between_requests, 0)
        self.assertEqual(worker.delay, 15)
        self.assertEqual(worker.exit, False)
        worker.shutdown()
        del worker

    @patch('gevent.sleep')
    def test_worker_qualification(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.client.request.return_value = ResponseMock({'X-Request-ID': self.request_ids[0]},
                                                        munchify(
                                                            {'prev_page': {'offset': '123'},
                                                             'next_page': {'offset': '1234'},
                                                             'data': {'status': "active.pre-qualification",
                                                                      'id': self.tender_id,
                                                                      'procurementMethodType': 'aboveThresholdEU',
                                                                      'bids': [self.bids(0, CODES[0]),
                                                                               self.bids(1, CODES[1]),
                                                                               self.bids(2, CODES[2]),
                                                                               self.bids(3, CODES[2]),
                                                                               {'id': self.bid_ids[4],
                                                                                'tenderers': [{'identifier': {
                                                                                    'scheme': 'UA-ED',
                                                                                    'id': CODES[2]}}]}],
                                                                      'qualifications': [
                                                                          self.qualifications('pending', 0, 0),
                                                                          self.qualifications('pending', 1, 1),
                                                                          self.qualifications('pending', 2, 2),
                                                                          self.qualifications('unsuccessful', 3, 3),
                                                                          self.qualifications('pending', 4, 4)]}}))
        for i in range(3):
            data = Data(self.tender_id, self.qualification_ids[i], CODES[i], 'qualifications',
                        {'meta': {'sourceRequests': [self.request_ids[0]]}})
            self.check_data_objects(self.edrpou_codes_queue.get(), data)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(),
                              [item_key(self.tender_id, self.qualification_ids[i]) for i in range(3)])

    @patch('gevent.sleep')
    def test_worker_award(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.client.request.side_effect = [ResponseMock({'X-Request-ID': self.request_ids[0]},
                                                        munchify({'prev_page': {'offset': '123'},
                                                                  'next_page': {'offset': '1234'},
                                                                  'data': {'status': "active.pre-qualification",
                                                                           'id': self.tender_id,
                                                                           'procurementMethodType': 'aboveThresholdEU',
                                                                           'awards': [
                                                                               self.awards(0, 0, 'pending', CODES[0]),
                                                                               self.awards(1, 1, 'pending', CODES[1]),
                                                                               self.awards(2, 2, 'pending', CODES[2]),
                                                                               self.awards(3, 3, 'unsuccessful',
                                                                                           CODES[2]),
                                                                               {'id': self.bid_ids[4],
                                                                                'tenderers': [{'identifier': {
                                                                                    'scheme': 'UA-ED',
                                                                                    'id': CODES[2]}}]}]}}))]
        for i in range(3):
            data = Data(self.tender_id, self.award_ids[i], CODES[i], 'awards',
                        {'meta': {'sourceRequests': [self.request_ids[0]]}})
            self.check_data_objects(self.edrpou_codes_queue.get(), data)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(),
                              [item_key(self.tender_id, self.award_ids[0]), item_key(self.tender_id, self.award_ids[1]),
                               item_key(self.tender_id, self.award_ids[2])])

    @patch('gevent.sleep')
    def test_get_tender_exception(self, gevent_sleep):
        """ We must not lose tender after restart filter worker """
        gevent_sleep.side_effect = custom_sleep
        self.client.request.side_effect = [Exception(), self.response]
        data = Data(self.tender_id, self.award_ids[0], CODES[0], 'awards',
                    {'meta': {'sourceRequests': [self.request_ids[0]]}})
        self.check_data_objects(self.edrpou_codes_queue.get(), data)
        self.assertEqual(self.worker.sleep_change_value.time_between_requests, 0)
        gevent_sleep.assert_called_with_once(1)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(),
                              [item_key(self.tender_id, self.award_ids[0])])
        self.assertEqual(self.edrpou_codes_queue.qsize(), 0)

    @patch('gevent.sleep')
    def test_get_tender_429(self, gevent_sleep):
        """ We must not lose tender after restart filter worker """
        gevent_sleep.side_effect = custom_sleep
        self.client.request.side_effect = [ResourceError(http_code=429), self.response]
        data = Data(self.tender_id, self.award_ids[0], CODES[0], 'awards',
                    {'meta': {'sourceRequests': [self.request_ids[0]]}})
        self.sleep_change_value.increment_step = 2
        self.sleep_change_value.decrement_step = 1
        self.check_data_objects(self.edrpou_codes_queue.get(), data)
        self.assertEqual(self.worker.sleep_change_value.time_between_requests, 1)
        gevent_sleep.assert_called_with_once(1)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(),
                              [item_key(self.tender_id, self.award_ids[0])])
        self.assertEqual(self.edrpou_codes_queue.qsize(), 0)

    @patch('gevent.sleep')
    def test_worker_restart(self, gevent_sleep):
        """ Process tender after catch Unauthorized exception """
        gevent_sleep.side_effect = custom_sleep
        self.client.request.side_effect = [
            Unauthorized(http_code=403),
            Unauthorized(http_code=403),
            Unauthorized(http_code=403),
            ResponseMock({'X-Request-ID': self.request_ids[0]},
                         munchify({'prev_page': {'offset': '123'},
                                   'next_page': {'offset': '1234'},
                                   'data': {'status': "active.pre-qualification",
                                            'id': self.tender_id,
                                            'procurementMethodType': 'aboveThresholdEU',
                                            'awards': [self.awards(0, 0, 'pending', CODES[0]),
                                                       self.awards(1, 1, 'unsuccessful', CODES[2])]}}))]
        data = Data(self.tender_id, self.award_ids[0], CODES[0], 'awards',
                    {'meta': {'sourceRequests': [self.request_ids[0]]}})
        self.check_data_objects(self.edrpou_codes_queue.get(), data)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(),
                              [item_key(self.tender_id, self.award_ids[0])])

    @patch('gevent.sleep')
    def test_worker_dead(self, gevent_sleep):
        """ Test that worker will process tender after exception  """
        gevent_sleep.side_effect = custom_sleep
        self.filtered_tender_ids_queue.put(self.tender_id)
        self.client.request.side_effect = [
            ResponseMock({'X-Request-ID': self.request_ids[i]},
                         munchify({'prev_page': {'offset': '123'},
                                   'next_page': {'offset': '1234'},
                                   'data': {
                                       'status': "active.pre-qualification",
                                       'id': self.tender_id,
                                       'procurementMethodType': 'aboveThresholdEU',
                                       'awards': [self.awards(i, i, 'pending', CODES[0])]}})) for i in range(2)]
        for i in range(2):
            data = Data(self.tender_id, self.award_ids[i], CODES[0], 'awards',
                        {'meta': {'sourceRequests': [self.request_ids[i]]}})
            self.check_data_objects(self.edrpou_codes_queue.get(), data)
        self.worker.immortal_jobs['prepare_data'].kill(timeout=1)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(),
                              [item_key(self.tender_id, self.award_ids[i]) for i in range(2)])

    @patch('gevent.sleep')
    def test_filtered_tender_ids_queue_loop_exit(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        filtered_tender_ids_queue = MagicMock()
        filtered_tender_ids_queue.peek.side_effect = [LoopExit(), self.tender_id]
        self.client.request.return_value = self.response
        first_data = Data(self.tender_id, self.award_ids[0], CODES[0], 'awards',
                          {'meta': {'sourceRequests': [self.request_ids[0]]}})
        self.worker.filtered_tender_ids_queue = filtered_tender_ids_queue
        self.check_data_objects(self.edrpou_codes_queue.get(), first_data)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(),
                              [item_key(self.tender_id, self.award_ids[0])])

    @patch('gevent.sleep')
    def test_worker_award_with_cancelled_lot(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.client.request.return_value = ResponseMock({'X-Request-ID': self.request_ids[0]},
                                                        munchify({'prev_page': {'offset': '123'},
                                                                  'next_page': {'offset': '1234'},
                                                                  'data': {'status': "active.pre-qualification",
                                                                           'id': self.tender_id,
                                                                           'procurementMethodType': 'aboveThresholdEU',
                                                                           'lots': [{'status': 'cancelled',
                                                                                     'id': '123456789'},
                                                                                    {'status': 'active',
                                                                                     'id': '12345678'}],
                                                                           'awards': [{'id': self.award_ids[0],
                                                                                       'bid_id': self.bid_ids[0],
                                                                                       'status': 'pending',
                                                                                       'suppliers': [{'identifier': {
                                                                                           'scheme': 'UA-EDR',
                                                                                           'id': CODES[0]}}],
                                                                                       'lotID': '123456789'},
                                                                                      {'id': self.award_ids[1],
                                                                                       'bid_id': self.bid_ids[1],
                                                                                       'status': 'pending',
                                                                                       'suppliers': [{'identifier': {
                                                                                           'scheme': 'UA-EDR',
                                                                                           'id': CODES[1]}}],
                                                                                       'lotID': '12345678'}]}}))
        data = Data(self.tender_id, self.award_ids[1], CODES[1], 'awards',
                    {'meta': {'sourceRequests': [self.request_ids[0]]}})
        for edrpou in [data]:
            self.check_data_objects(self.edrpou_codes_queue.get(), edrpou)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(),
                              [item_key(self.tender_id, self.award_ids[1])])

    @patch('gevent.sleep')
    def test_qualification_not_valid_identifier_id(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.client.request.return_value = ResponseMock({'X-Request-ID': self.request_ids[0]},
                                                        munchify(
                                                            {'prev_page': {'offset': '123'},
                                                             'next_page': {'offset': '1234'},
                                                             'data': {'status': "active.pre-qualification",
                                                                      'id': self.tender_id,
                                                                      'procurementMethodType': 'aboveThresholdEU',
                                                                      'bids': [self.bids(0, 'test@test.com'),
                                                                               self.bids(1, ''),
                                                                               self.bids(1, 'абв'),
                                                                               self.bids(1, u'абв')],
                                                                      'qualifications': [
                                                                          self.qualifications('pending', 0, 0),
                                                                          self.qualifications('pending', 1, 1)]}}))
        sleep(1)
        self.assertEqual(self.edrpou_codes_queue.qsize(), 0)
        self.assertItemsEqual(self.process_tracker.processing_items, {})

    @patch('gevent.sleep')
    def test_award_not_valid_identifier_id(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.client.request.return_value = ResponseMock({'X-Request-ID': self.request_ids[0]},
                                                        munchify({'prev_page': {'offset': '123'},
                                                                  'next_page': {'offset': '1234'},
                                                                  'data': {'status': "active.pre-qualification",
                                                                           'id': self.tender_id,
                                                                           'procurementMethodType': 'aboveThresholdEU',
                                                                           'awards': [self.awards(0, 0, 'pending', ''),
                                                                                      self.awards(0, 0, 'pending',
                                                                                                  u'абв'),
                                                                                      self.awards(0, 1, 'pending',
                                                                                                  'абв'),
                                                                                      self.awards(1, 1, 'pending',
                                                                                                  'test@test.com')]}}))
        sleep(1)
        self.assertEqual(self.edrpou_codes_queue.qsize(), 0)
        self.assertItemsEqual(self.process_tracker.processing_items, {})

    @patch('gevent.sleep')
    def test_412(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.worker.kill()
        filtered_tender_ids_queue = Queue(10)
        filtered_tender_ids_queue.put('123')
        setup_routing(self.api_server_bottle, response_spore)
        setup_routing(self.api_server_bottle, generate_response, path='/api/2.3/tenders/123')
        client = TendersClientSync('', host_url='http://127.0.0.1:20604', api_version='2.3')
        self.assertEqual(client.headers['Cookie'],
                         'SERVER_ID={}'.format(SPORE_COOKIES))  # check that response_spore set cookies
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, self.edrpou_codes_queue, self.process_tracker,
                                     MagicMock(), self.sleep_change_value)
        data = Data('123', '124', CODES[0], 'awards', {'meta': {'sourceRequests': ['125']}})
        for i in [data]:
            self.check_data_objects(self.edrpou_codes_queue.get(), i)
        self.assertEqual(client.headers['Cookie'],
                         'SERVER_ID={}'.format(COOKIES_412))  # check that response_412 change cookies
        self.assertEqual(self.edrpou_codes_queue.qsize(), 0)
        self.assertItemsEqual(self.process_tracker.processing_items.keys(), ['123_124'])
        worker.shutdown()
        del worker

    @patch('gevent.sleep')
    def test_request_failed(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        self.client.request.side_effect = [RequestFailed(http_code=401, msg=RequestFailed()),
                                           ResponseMock({'X-Request-ID': self.request_ids[0]},
                                                        munchify({'prev_page': {'offset': '123'},
                                                                  'next_page': {'offset': '1234'},
                                                                  'data': {'status': "active.pre-qualification",
                                                                           'id': self.tender_id,
                                                                           'procurementMethodType': 'aboveThresholdEU',
                                                                           'awards': [
                                                                               self.awards(0, 0, 'pending', '')]}}))]
        sleep(1)
        self.assertEqual(self.client.request.call_count, 2)
        self.assertEqual(self.edrpou_codes_queue.qsize(), 0)
        self.assertItemsEqual(self.process_tracker.processing_items, {})
