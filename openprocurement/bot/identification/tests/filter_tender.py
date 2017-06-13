# -*- coding: utf-8 -*-
import uuid
import unittest
import datetime
from gevent.hub import LoopExit
from gevent.queue import Queue
from openprocurement.bot.identification.databridge.constants import author
from openprocurement.bot.identification.databridge.filter_tender import FilterTenders
from openprocurement.bot.identification.databridge.utils import Data
from openprocurement.bot.identification.tests.utils import custom_sleep, generate_request_id, ResponseMock
from openprocurement.bot.identification.databridge.bridge import TendersClientSync
from mock import patch, MagicMock
from time import sleep
from munch import munchify
from restkit.errors import Unauthorized, ResourceError
from gevent.pywsgi import WSGIServer
from bottle import Bottle, response
from simplejson import dumps

SERVER_RESPONSE_FLAG = 0
SPORE_COOKIES = ("a7afc9b1fc79e640f2487ba48243ca071c07a823d27"
                 "8cf9b7adf0fae467a524747e3c6c6973262130fac2b"
                 "96a11693fa8bd38623e4daee121f60b4301aef012c")
COOKIES_412 = ("b7afc9b1fc79e640f2487ba48243ca071c07a823d27"
               "8cf9b7adf0fae467a524747e3c6c6973262130fac2b"
               "96a11693fa8bd38623e4daee121f60b4301aef012c")


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
                                       'status': 'pending',
                                       'suppliers': [{'identifier': {
                                                      'scheme': 'UA-EDR',
                                                      'id': '14360570'}}]}]}})


def generate_response():
    global SERVER_RESPONSE_FLAG
    if SERVER_RESPONSE_FLAG == 0:
        SERVER_RESPONSE_FLAG = 1
        return response_412()
    return response_get_tender()


class TestFilterWorker(unittest.TestCase):

    def check_data_objects(self, obj, example):
        """Checks that two data objects are equal, 
                  that Data.file_content.meta.id is not none and
                  that Data.file_content.meta.author exists and is equal to IdentificationBot
         """
        self.assertEqual(obj.tender_id, example.tender_id)
        self.assertEqual(obj.item_id, example.item_id)
        self.assertEqual(obj.code, example.code)
        self.assertEqual(obj.item_name, example.item_name)
        self.assertEqual(obj.edr_ids, example.edr_ids)
        self.assertIsNotNone(obj.file_content['meta']['id'])
        self.assertEqual(obj.file_content['meta']['author'], author)
        self.assertEqual(obj.file_content['meta']['sourceRequests'], example.file_content['meta']['sourceRequests'])

    def test_init(self):
        worker = FilterTenders.spawn(None, None, None, None)
        self.assertGreater(datetime.datetime.now().isoformat(),
                           worker.start_time.isoformat())
        self.assertEqual(worker.tenders_sync_client, None)
        self.assertEqual(worker.filtered_tender_ids_queue, None)
        self.assertEqual(worker.edrpou_codes_queue, None)
        self.assertEqual(worker.processing_items, None)
        self.assertEqual(worker.delay, 15)
        self.assertEqual(worker.exit, False)

        worker.shutdown()
        del worker

    @patch('gevent.sleep')
    def test_worker_qualification(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        filtered_tender_ids_queue = Queue(10)
        edrpou_codes_queue = Queue(10)
        processing_items = {}
        request_id = generate_request_id()
        tender_id = uuid.uuid4().hex
        filtered_tender_ids_queue.put(tender_id)
        first_bid_id, second_bid_id, third_bid_id, forth_bid_id, fifth_bid_id = [uuid.uuid4().hex for i in range(5)]
        first_qualification_id, second_qualification_id, third_qualification_id, fourth_qualification_id, fifth_qualification_id = [uuid.uuid4().hex for i in range(5)]
        client = MagicMock()
        client.request.return_value = ResponseMock({'X-Request-ID': request_id},
                                                    munchify(
                                                        {'prev_page': {'offset': '123'},
                                                         'next_page': {'offset': '1234'},
                                                         'data': {'status': "active.pre-qualification",
                                                               'id': tender_id,
                                                               'procurementMethodType': 'aboveThresholdEU',
                                                               'bids': [{'id': first_bid_id,
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': '14360570'}
                                                                         }]},
                                                                        {'id': second_bid_id,
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': '0013823'}
                                                                         }]},
                                                                        {'id': third_bid_id,
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': '23494714'}
                                                                         }]},
                                                                        {'id': forth_bid_id,
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': '23494714'}
                                                                         }]},
                                                                        {'id': fifth_bid_id,
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-ED',
                                                                             'id': '23494714'}
                                                                         }]},
                                                                        ],
                                                               'qualifications': [{'status': 'pending',
                                                                                   'id': first_qualification_id,
                                                                                   'bidID': first_bid_id},
                                                                                  {'status': 'pending',
                                                                                   'id': second_qualification_id,
                                                                                   'bidID': second_bid_id},
                                                                                  {'status': 'pending',
                                                                                   'id': third_qualification_id,
                                                                                   'bidID': third_bid_id},
                                                                                  {'status': 'unsuccessful',
                                                                                   'id': fourth_qualification_id,
                                                                                   'bidID': forth_bid_id},
                                                                                  {'status': 'pending',
                                                                                   'id': fifth_qualification_id,
                                                                                   'bidID': fifth_bid_id},
                                                                                  ]}}))
        first_data = Data(tender_id, first_qualification_id, '14360570', 'qualifications', None, {'meta': {'sourceRequests': [request_id]}})
        second_data = Data(tender_id, second_qualification_id, '0013823', 'qualifications', None, {'meta': {'sourceRequests': [request_id]}})
        third_data = Data(tender_id, third_qualification_id, '23494714', 'qualifications', None, {'meta': {'sourceRequests': [request_id]}})
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items)

        for data in [first_data, second_data, third_data]:
            self.check_data_objects(edrpou_codes_queue.get(), data)

        worker.shutdown()
        del worker

        self.assertItemsEqual(processing_items.keys(),
                              ['{}_{}'.format(tender_id, first_qualification_id),
                               '{}_{}'.format(tender_id, second_qualification_id),
                               '{}_{}'.format(tender_id, third_qualification_id)])

    @patch('gevent.sleep')
    def test_worker_award(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        filtered_tender_ids_queue = Queue(10)
        edrpou_codes_queue = Queue(10)
        processing_items = {}
        tender_id = uuid.uuid4().hex
        request_id = generate_request_id()
        filtered_tender_ids_queue.put(tender_id)
        first_award_id, second_award_id, third_award_id, fourth_award_id, fifth_award_id = [uuid.uuid4().hex for i in range(5)]
        client = MagicMock()
        client.request.side_effect = [ResponseMock({'X-Request-ID': request_id},
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': {'status': "active.pre-qualification",
                               'id': tender_id,
                               'procurementMethodType': 'aboveThresholdEU',
                               'awards': [{'id': first_award_id,
                                           'status': 'pending',
                                           'suppliers': [{'identifier': {
                                             'scheme': 'UA-EDR',
                                             'id': '14360570'}
                                         }]},
                                        {'id': second_award_id,
                                         'status': 'pending',
                                         'suppliers': [{'identifier': {
                                             'scheme': 'UA-EDR',
                                             'id': '0013823'}
                                         }]},
                                        {'id': third_award_id,
                                         'status': 'pending',
                                         'suppliers': [{'identifier': {
                                             'scheme': 'UA-EDR',
                                             'id': '23494714'}
                                         }]},
                                        {'id': fourth_award_id,
                                         'status': 'unsuccessful',
                                         'suppliers': [{'identifier': {
                                            'scheme': 'UA-EDR',
                                            'id': '23494714'}
                                         }]},
                                          {'id': fifth_award_id,
                                           'status': 'pending',
                                           'suppliers': [{'identifier': {
                                               'scheme': 'UA-ED',
                                               'id': '23494714'}
                                           }]},
                                        ]
                               }}))]

        first_data = Data(tender_id, first_award_id, '14360570', 'awards', None, {'meta': {'sourceRequests': [request_id]}})
        second_data = Data(tender_id, second_award_id, '0013823', 'awards', None, {'meta': {'sourceRequests': [request_id]}})
        third_data = Data(tender_id, third_award_id, '23494714', 'awards', None, {'meta': {'sourceRequests': [request_id]}})
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items, 2, 1)
        for edrpou in [first_data, second_data, third_data]:
            self.check_data_objects(edrpou_codes_queue.get(), edrpou)

        worker.shutdown()
        del worker

        self.assertItemsEqual(processing_items.keys(),
                              ['{}_{}'.format(tender_id, first_award_id),
                               '{}_{}'.format(tender_id, second_award_id),
                               '{}_{}'.format(tender_id, third_award_id)])

    @patch('gevent.sleep')
    def test_get_tender_exception(self, gevent_sleep):
        """ We must not lose tender after restart filter worker """
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        request_id = generate_request_id()
        award_id = uuid.uuid4().hex
        filtered_tender_ids_queue = Queue(10)
        filtered_tender_ids_queue.put(tender_id)
        edrpou_codes_queue = Queue(10)
        processing_items = {}
        client = MagicMock()
        client.request.side_effect = [Exception(),
                                      ResponseMock({'X-Request-ID': request_id},
                                                   munchify({'prev_page': {'offset': '123'},
                                                             'next_page': {'offset': '1234'},
                                                             'data': {'status': "active.pre-qualification",
                                                                      'id': tender_id,
                                                                      'procurementMethodType': 'aboveThresholdEU',
                                                                      'awards': [{'id': award_id,
                                                                                  'status': 'pending',
                                                                                  'suppliers': [{'identifier': {
                                                                                      'scheme': 'UA-EDR',
                                                                                      'id': '14360570'}
                                                                                  }]}
                                                                                 ]
                                                                      }}))]
        data = Data(tender_id, award_id, '14360570', 'awards', None, {'meta': {'sourceRequests': [request_id]}})
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items, 2, 1)
        self.check_data_objects(edrpou_codes_queue.get(), data)
        worker.shutdown()
        self.assertEqual(worker.sleep_change_value, 0)
        del worker
        gevent_sleep.assert_called_with_once(1)
        self.assertItemsEqual(processing_items.keys(), ['{}_{}'.format(tender_id, award_id)])
        self.assertEqual(edrpou_codes_queue.qsize(), 0)

    @patch('gevent.sleep')
    def test_get_tender_429(self, gevent_sleep):
        """ We must not lose tender after restart filter worker """
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        request_id = generate_request_id()
        award_id = uuid.uuid4().hex
        filtered_tender_ids_queue = Queue(10)
        filtered_tender_ids_queue.put(tender_id)
        edrpou_codes_queue = Queue(10)
        processing_items = {}
        client = MagicMock()
        client.request.side_effect = [
                                      ResourceError(http_code=429),
                                      ResponseMock({'X-Request-ID': request_id},
                                                   munchify({'prev_page': {'offset': '123'},
                                                             'next_page': {'offset': '1234'},
                                                             'data': {'status': "active.pre-qualification",
                                                                      'id': tender_id,
                                                                      'procurementMethodType': 'aboveThresholdEU',
                                                                      'awards': [{'id': award_id,
                                                                                  'status': 'pending',
                                                                                  'suppliers': [{'identifier': {
                                                                                      'scheme': 'UA-EDR',
                                                                                      'id': '14360570'}
                                                                                  }]}
                                                                                 ]
                                                                      }}))]
        data = Data(tender_id, award_id, '14360570', 'awards', None, {'meta': {'sourceRequests': [request_id]}})
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items, 2, 1)
        self.check_data_objects(edrpou_codes_queue.get(), data)
        worker.shutdown()
        self.assertEqual(worker.sleep_change_value, 1)
        del worker
        gevent_sleep.assert_called_with_once(1)
        self.assertItemsEqual(processing_items.keys(), ['{}_{}'.format(tender_id, award_id)])
        self.assertEqual(edrpou_codes_queue.qsize(), 0)

    @patch('gevent.sleep')
    def test_worker_restart(self, gevent_sleep):
        """ Process tender after catch Unauthorized exception """
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        filtered_tender_ids_queue = Queue(10)
        filtered_tender_ids_queue.put(tender_id)
        request_id = generate_request_id()
        first_award_id, second_award_id = (uuid.uuid4().hex, uuid.uuid4().hex)
        edrpou_codes_queue = Queue(10)
        processing_items = {}
        client = MagicMock()
        client.request.side_effect = [
            Unauthorized(http_code=403),
            Unauthorized(http_code=403),
            Unauthorized(http_code=403),
            ResponseMock({'X-Request-ID': request_id},
                    munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': {'status': "active.pre-qualification",
                               'id': tender_id,
                               'procurementMethodType': 'aboveThresholdEU',
                               'awards': [{'id': first_award_id,
                                           'status': 'pending',
                                           'suppliers': [{'identifier': {
                                               'scheme': 'UA-EDR',
                                               'id': '14360570'}
                                           }]},
                                          {'id': second_award_id,
                                           'status': 'unsuccessful',
                                           'suppliers': [{'identifier': {
                                               'scheme': 'UA-EDR',
                                               'id': '23494714'}
                                           }]},
                                          ]
                               }}))
        ]
        data = Data(tender_id, first_award_id, '14360570', 'awards', None, {'meta': {'sourceRequests': [request_id]}})
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items)

        self.check_data_objects(edrpou_codes_queue.get(), data)

        worker.shutdown()
        del worker

        self.assertItemsEqual(processing_items.keys(), ['{}_{}'.format(tender_id, first_award_id)])

    @patch('gevent.sleep')
    def test_worker_dead(self, gevent_sleep):
        """ Test that worker will process tender after exception  """
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        filtered_tender_ids_queue = Queue(10)
        filtered_tender_ids_queue.put(tender_id)
        filtered_tender_ids_queue.put(tender_id)
        first_award_id = uuid.uuid4().hex
        second_award_id = uuid.uuid4().hex
        first_request_id = generate_request_id()
        second_request_id = generate_request_id()
        edrpou_codes_queue = Queue(10)
        processing_items = {}
        client = MagicMock()
        client.request.side_effect = [
            ResponseMock({'X-Request-ID': first_request_id},
                munchify(
                {'prev_page': {'offset': '123'},
                 'next_page': {'offset': '1234'},
                 'data': {
                     'status': "active.pre-qualification",
                     'id': tender_id,
                     'procurementMethodType': 'aboveThresholdEU',
                     'awards': [
                         {'id': first_award_id,
                          'status': 'pending',
                          'suppliers': [
                              {'identifier': {'scheme': 'UA-EDR',
                                              'id': '14360570'}}]
                          }]}})),
            ResponseMock({'X-Request-ID': second_request_id},
                munchify(
                {'prev_page': {'offset': '123'},
                 'next_page': {'offset': '1234'},
                 'data': {
                     'status': "active.pre-qualification",
                     'id': tender_id,
                     'procurementMethodType': 'aboveThresholdEU',
                     'awards': [
                         {'id': second_award_id,
                          'status': 'pending',
                          'suppliers': [
                              {'identifier': {'scheme': 'UA-EDR',
                                              'id': '14360570'}
                               }]}]}}))]
        first_data = Data(tender_id, first_award_id, '14360570', 'awards', None, {'meta': {'sourceRequests': [first_request_id]}})
        second_data = Data(tender_id, second_award_id, '14360570', 'awards', None, {'meta': {'sourceRequests': [second_request_id]}})
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue,
                                     edrpou_codes_queue, processing_items)
        self.check_data_objects(edrpou_codes_queue.get(), first_data)
        worker.job.kill(timeout=1)
        self.check_data_objects(edrpou_codes_queue.get(), second_data)

        worker.shutdown()
        del worker

        self.assertItemsEqual(processing_items.keys(), ['{}_{}'.format(tender_id, first_award_id),
                                                        '{}_{}'.format(tender_id, second_award_id)])

    @patch('gevent.sleep')
    def test_filtered_tender_ids_queue_loop_exit(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        filtered_tender_ids_queue = MagicMock()
        filtered_tender_ids_queue.peek.side_effect = [LoopExit(), tender_id]
        request_id = generate_request_id()
        first_award_id = uuid.uuid4().hex
        edrpou_codes_queue = Queue(10)
        processing_items = {}
        client = MagicMock()
        client.request.return_value = ResponseMock({'X-Request-ID': request_id},
            munchify(
                {'prev_page': {'offset': '123'},
                 'next_page': {'offset': '1234'},
                 'data': {
                     'status': "active.pre-qualification",
                     'id': tender_id,
                     'procurementMethodType': 'aboveThresholdEU',
                     'awards': [
                         {'id': first_award_id,
                          'status': 'pending',
                          'suppliers': [
                              {'identifier': {'scheme': 'UA-EDR',
                                              'id': '14360570'}}]
                          }]}}))
        first_data = Data(tender_id, first_award_id, '14360570', 'awards', None, {'meta': {'sourceRequests': [request_id]}})
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items)
        self.check_data_objects(edrpou_codes_queue.get(), first_data)

        worker.shutdown()
        del worker

        self.assertItemsEqual(processing_items.keys(),
                              ['{}_{}'.format(tender_id, first_award_id)])

    @patch('gevent.sleep')
    def test_worker_award_with_cancelled_lot(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        filtered_tender_ids_queue = Queue(10)
        edrpou_codes_queue = Queue(10)
        processing_items = {}
        tender_id = uuid.uuid4().hex
        request_id = generate_request_id()
        filtered_tender_ids_queue.put(tender_id)
        first_award_id, second_award_id = [uuid.uuid4().hex for i in range(2)]
        client = MagicMock()
        client.request.return_value = ResponseMock({'X-Request-ID': request_id},
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': {'status': "active.pre-qualification",
                               'id': tender_id,
                               'procurementMethodType': 'aboveThresholdEU',
                               'lots': [{'status': 'cancelled',
                                         'id': '123456789'},
                                        {'status': 'active',
                                         'id': '12345678'}
                                        ],
                               'awards': [{'id': first_award_id,
                                           'status': 'pending',
                                           'suppliers': [{'identifier': {
                                             'scheme': 'UA-EDR',
                                             'id': '14360570'}
                                         }],
                                           'lotID': '123456789'},
                                        {'id': second_award_id,
                                         'status': 'pending',
                                         'suppliers': [{'identifier': {
                                             'scheme': 'UA-EDR',
                                             'id': '0013823'}
                                         }],
                                         'lotID': '12345678'}
                                        ]
                               }}))

        data = Data(tender_id, second_award_id, '0013823', 'awards', None, {'meta': {'sourceRequests': [request_id]}})
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items)

        for edrpou in [data]:
            self.check_data_objects(edrpou_codes_queue.get(), edrpou)

        worker.shutdown()
        del worker

        self.assertItemsEqual(processing_items.keys(), ['{}_{}'.format(tender_id, second_award_id)])


    @patch('gevent.sleep')
    def test_qualification_not_valid_identifier_id(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        filtered_tender_ids_queue = Queue(10)
        edrpou_codes_queue = Queue(10)
        processing_items = {}
        request_id = generate_request_id()
        tender_id = uuid.uuid4().hex
        filtered_tender_ids_queue.put(tender_id)
        first_bid_id, second_bid_id = [uuid.uuid4().hex for i in range(2)]
        first_qualification_id, second_qualification_id = [uuid.uuid4().hex for i in range(2)]
        client = MagicMock()
        client.request.return_value = ResponseMock({'X-Request-ID': request_id},
                                                    munchify(
                                                        {'prev_page': {'offset': '123'},
                                                         'next_page': {'offset': '1234'},
                                                         'data': {'status': "active.pre-qualification",
                                                               'id': tender_id,
                                                               'procurementMethodType': 'aboveThresholdEU',
                                                               'bids': [{'id': first_bid_id,
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': 'test@test.com'}
                                                                         }]},
                                                                        {'id': second_bid_id,
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': ''}
                                                                         }]},
                                                                        {'id': second_bid_id,
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': 'абв'}
                                                                         }]},
                                                                        {'id': second_bid_id,
                                                                         'tenderers': [{'identifier': {
                                                                             'scheme': 'UA-EDR',
                                                                             'id': u'абв'}
                                                                         }]}
                                                                        ],
                                                               'qualifications': [{'status': 'pending',
                                                                                   'id': first_qualification_id,
                                                                                   'bidID': first_bid_id},
                                                                                  {'status': 'pending',
                                                                                   'id': second_qualification_id,
                                                                                   'bidID': second_bid_id}]}}))
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items)

        sleep(1)
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertItemsEqual(processing_items, {})

        worker.shutdown()
        del worker

    @patch('gevent.sleep')
    def test_award_not_valid_identifier_id(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        filtered_tender_ids_queue = Queue(10)
        edrpou_codes_queue = Queue(10)
        processing_items = {}
        tender_id = uuid.uuid4().hex
        request_id = generate_request_id()
        filtered_tender_ids_queue.put(tender_id)
        first_award_id, second_award_id = [uuid.uuid4().hex for i in range(2)]
        client = MagicMock()
        client.request.return_value = ResponseMock({'X-Request-ID': request_id},
                                                   munchify({'prev_page': {'offset': '123'},
                                                             'next_page': {'offset': '1234'},
                                                             'data': {'status': "active.pre-qualification",
                                                                      'id': tender_id,
                                                                      'procurementMethodType': 'aboveThresholdEU',
                                                                      'awards': [{'id': first_award_id,
                                                                                  'status': 'pending',
                                                                                  'suppliers': [{'identifier': {
                                                                                      'scheme': 'UA-EDR',
                                                                                      'id': ''}
                                                                                  }]},
                                                                                 {'id': first_award_id,
                                                                                  'status': 'pending',
                                                                                  'suppliers': [{'identifier': {
                                                                                      'scheme': 'UA-EDR',
                                                                                      'id': u'абв'}
                                                                                  }]},
                                                                                 {'id': first_award_id,
                                                                                  'status': 'pending',
                                                                                  'suppliers': [{'identifier': {
                                                                                      'scheme': 'UA-EDR',
                                                                                      'id': 'абв'}
                                                                                  }]},
                                                                                 {'id': second_award_id,
                                                                                  'status': 'pending',
                                                                                  'suppliers': [{'identifier': {
                                                                                      'scheme': 'UA-EDR',
                                                                                      'id': 'test@test.com'}
                                                                                  }]}]
                                                                      }}))
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items)

        sleep(1)

        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertItemsEqual(processing_items, {})

        worker.shutdown()
        del worker

    @patch('gevent.sleep')
    def test_412(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        filtered_tender_ids_queue = Queue(10)
        edrpou_codes_queue = Queue(10)
        processing_items = {}
        filtered_tender_ids_queue.put('123')
        api_server_bottle = Bottle()
        api_server = WSGIServer(('127.0.0.1', 20604), api_server_bottle, log=None)
        setup_routing(api_server_bottle, response_spore)
        setup_routing(api_server_bottle, generate_response, path='/api/2.3/tenders/123')
        api_server.start()
        client = TendersClientSync('', host_url='http://127.0.0.1:20604', api_version='2.3')
        self.assertEqual(client.headers['Cookie'], 'SERVER_ID={}'.format(SPORE_COOKIES))  # check that response_spore set cookies
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items)
        data = Data('123', '124', '14360570', 'awards', None, {'meta': {'sourceRequests': ['125']}})

        for i in [data]:
            self.check_data_objects(edrpou_codes_queue.get(), i)
        self.assertEqual(client.headers['Cookie'], 'SERVER_ID={}'.format(COOKIES_412))  # check that response_412 change cookies
        self.assertEqual(edrpou_codes_queue.qsize(), 0)
        self.assertItemsEqual(processing_items.keys(), ['123_124'])

        worker.shutdown()
        del worker
        api_server.stop()
