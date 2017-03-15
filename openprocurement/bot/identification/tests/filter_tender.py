# -*- coding: utf-8 -*-
import uuid
import unittest
import datetime
from gevent.queue import Queue
from gevent import sleep as gsleep
from openprocurement.bot.identification.databridge.filter_tender import FilterTenders
from openprocurement.bot.identification.databridge.utils import Data
from mock import patch, MagicMock
from time import sleep
from munch import munchify
from restkit.errors import Unauthorized


def custom_sleep(seconds):
    return gsleep(seconds=0)


class TestFilterWorker(unittest.TestCase):

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
        tender_id = uuid.uuid4().hex
        filtered_tender_ids_queue.put(tender_id)
        first_bid_id, second_bid_id, third_bid_id, forth_bid_id, fifth_bid_id = (uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex)
        first_qualification_id, second_qualification_id, third_qualification_id, fourth_qualification_id, fifth_qualification_id = (uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex)
        client = MagicMock()
        client.get_tender.side_effect = [
            munchify({'prev_page': {'offset': '123'},
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
                                                   ]
                                }}),
            ]

        first_data = Data(tender_id, first_qualification_id, '14360570', 'qualifications', None, None)
        second_data = Data(tender_id, second_qualification_id, '0013823', 'qualifications', None, None)
        third_data = Data(tender_id, third_qualification_id, '23494714', 'qualifications', None, None)
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items)
        sleep(4)
        worker.shutdown()
        del worker

        self.assertEqual(edrpou_codes_queue.qsize(), 3)
        self.assertEqual(edrpou_codes_queue.get(), first_data)
        self.assertEqual(edrpou_codes_queue.get(), second_data)
        self.assertEqual(edrpou_codes_queue.get(), third_data)
        self.assertItemsEqual(processing_items.keys(), [first_qualification_id, second_qualification_id, third_qualification_id])

    @patch('gevent.sleep')
    def test_worker_award(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        filtered_tender_ids_queue = Queue(10)
        edrpou_codes_queue = Queue(10)
        processing_items = {}
        tender_id = uuid.uuid4().hex
        filtered_tender_ids_queue.put(tender_id)
        first_award_id, second_award_id, third_award_id, fourth_award_id, fifth_award_id = \
            (uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex)
        client = MagicMock()
        client.get_tender.side_effect = [
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
                               }}),
        ]

        first_data = Data(tender_id, first_award_id, '14360570', 'awards', None, None)
        second_data = Data(tender_id, second_award_id, '0013823', 'awards', None, None)
        third_data = Data(tender_id, third_award_id, '23494714', 'awards', None, None)
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items)
        sleep(4)
        worker.shutdown()
        del worker

        self.assertEqual(edrpou_codes_queue.qsize(), 3)
        self.assertEqual(edrpou_codes_queue.get(), first_data)
        self.assertEqual(edrpou_codes_queue.get(), second_data)
        self.assertEqual(edrpou_codes_queue.get(), third_data)
        self.assertItemsEqual(processing_items.keys(), [first_award_id, second_award_id, third_award_id])

    @patch('gevent.sleep')
    def test_get_tender_exception(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        filtered_tender_ids_queue = Queue(10)
        filtered_tender_ids_queue.put(tender_id)
        edrpou_codes_queue = Queue(10)
        processing_items = {}
        client = MagicMock()
        client.get_tender.side_effect = [Unauthorized()]
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items)
        sleep(2)
        worker.shutdown()
        del worker

        self.assertEqual(filtered_tender_ids_queue.peek(), tender_id)
        self.assertEqual(processing_items, {})
        self.assertEqual(edrpou_codes_queue.qsize(), 0)

    @patch('gevent.sleep')
    def test_worker_restart(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        tender_id = uuid.uuid4().hex
        filtered_tender_ids_queue = Queue(10)
        filtered_tender_ids_queue.put(tender_id)
        first_award_id, second_award_id = (uuid.uuid4().hex, uuid.uuid4().hex)
        edrpou_codes_queue = Queue(10)
        processing_items = {}
        client = MagicMock()
        client.get_tender.side_effect = [Unauthorized(),
                                         Unauthorized(),
                                         Unauthorized(),
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
                                                            }}),
                                         ]
        data = Data(tender_id, first_award_id, '14360570', 'awards', None, None)
        worker = FilterTenders.spawn(client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items)
        sleep(2)
        worker.shutdown()
        del worker

        self.assertEqual(edrpou_codes_queue.qsize(), 1)
        self.assertEqual(edrpou_codes_queue.get(), data)
        self.assertItemsEqual(processing_items.keys(), [first_award_id])

