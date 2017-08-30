# -*- coding: utf-8 -*-
from gevent import monkey
from openprocurement.bot.identification.databridge.base_worker import BaseWorker

monkey.patch_all()

import logging.config
import gevent
from datetime import datetime
from gevent import spawn
from gevent.hub import LoopExit
from munch import munchify
from simplejson import loads

from openprocurement.bot.identification.databridge.utils import (
    generate_req_id, journal_context, generate_doc_id, is_code_invalid, item_id, journal_item_name,
    check_related_lot_status, journal_item_params)
from openprocurement.bot.identification.databridge.data import Data
from openprocurement.bot.identification.databridge.journal_msg_ids import (
    DATABRIDGE_GET_TENDER_FROM_QUEUE, DATABRIDGE_TENDER_PROCESS, DATABRIDGE_TENDER_NOT_PROCESS
)
from openprocurement.bot.identification.databridge.constants import author, identification_scheme, DOC_TYPE

logger = logging.getLogger(__name__)


class FilterTenders(BaseWorker):
    """ Edr API Data Bridge """

    def __init__(self, tenders_sync_client, filtered_tender_ids_queue, edrpou_codes_queue, process_tracker,
                 services_not_available, sleep_change_value, delay=15):
        super(FilterTenders, self).__init__(services_not_available)
        self.start_time = datetime.now()

        self.delay = delay
        self.process_tracker = process_tracker
        # init clients
        self.tenders_sync_client = tenders_sync_client

        # init queues for workers
        self.filtered_tender_ids_queue = filtered_tender_ids_queue
        self.edrpou_codes_queue = edrpou_codes_queue
        self.sleep_change_value = sleep_change_value

    def prepare_data(self):
        """Get tender_id from filtered_tender_ids_queue, check award/qualification status, documentType; get
        identifier's id and put into edrpou_codes_queue."""
        while not self.exit:
            self.services_not_available.wait()
            try:
                tender_id = self.filtered_tender_ids_queue.peek()
            except LoopExit:
                gevent.sleep()
            else:
                self.temp_process(tender_id)
            gevent.sleep(self.sleep_change_value.time_between_requests)

    def temp_process(self, tender_id):
        try:
            response = self.tenders_sync_client.request("GET", path='{}/{}'.format(self.tenders_sync_client.prefix_path,
                                                                                   tender_id),
                                                        headers={'X-Client-Request-ID': generate_req_id()})
        except Exception as e:
            if getattr(e, "status_int", False) == 429:
                self.sleep_change_value.increment()
                logger.info("Waiting tender {} for sleep_change_value: {} seconds".format(
                    tender_id, self.sleep_change_value.time_between_requests))
            else:
                logger.warning('Fail to get tender info {}'.format(tender_id),
                               extra=journal_context(params={"TENDER_ID": tender_id}))
                logger.exception("Message: {}".format(e.message))
        else:
            self.process_items_and_move(response, tender_id)

    def process_items_and_move(self, response, tender_id):
        self.sleep_change_value.decrement()
        if response.status_int == 200:
            tender = munchify(loads(response.body_string()))['data']
        logger.info('Get tender {} from filtered_tender_ids_queue'.format(tender_id),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                          params={"TENDER_ID": tender['id']}))
        if 'awards' in tender:
            self.process_items(response, tender, "award")
        elif 'qualifications' in tender:
            self.process_items(response, tender, "qualification")
        self.filtered_tender_ids_queue.get()

    def process_items(self, response, tender, item_name):
        for item in tender[item_name + "s"]:
            self.process_item(response, tender, tender['id'], item, item_name)

    def process_item(self, response, tender, tender_id, item, item_name):
        logger.info('Processing tender {} bid {} {} {}'.format(tender['id'], item_id(item), item_name, item['id']),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS}, journal_item_params(tender, item)))
        if self.should_process_item(item):
            if item_name == 'award':
                for supplier in item['suppliers']:
                    self.process_award_supplier(response, tender, item, supplier)
            elif item_name == 'qualification':
                self.process_qualification(response, tender, item)
        else:
            logger.info('Tender {} bid {} {} {} is not in status pending or award has already document '
                        'with documentType registerExtract.'.format(tender_id, item_id(item), item_name, item['id']),
                        extra=journal_context(params={"TENDER_ID": tender['id'], "BID_ID": item_id(item),
                                                      journal_item_name(item): item['id']}))

    def should_process_item(self, item):
        return item['status'] == 'pending' and self.has_not_documents(item)

    def has_not_documents(self, item):
        return not ([document for document in item.get('documents', []) if document.get('documentType') == DOC_TYPE])

    def temp_important_part_for_item(self, response, tender, item, item_name, code):
        self.process_tracker.set_item(tender['id'], item['id'])
        document_id = generate_doc_id()
        tender_data = Data(tender['id'], item['id'], str(code), item_name + "s",
                           {'meta': {'id': document_id, 'author': author,
                                     'sourceRequests': [response.headers['X-Request-ID']]}})
        self.edrpou_codes_queue.put(tender_data)

    def process_award_supplier(self, response, tender, award, supplier):
        code = supplier['identifier']['id']
        if is_code_invalid(code):
            self.remove_invalid_item(tender, award, "award", code)
        elif self.should_process_award(supplier, tender, award):
            self.temp_important_part_for_item(response, tender, award, "award", code)
        else:
            logger.info('Tender {} bid {} award {} identifier schema isn\'t UA-EDR, '
                        'tender is already in process or was processed.'.format(
                tender['id'], award['bid_id'], award['id']),
                extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_NOT_PROCESS},
                                      journal_item_params(tender, award)))

    def remove_invalid_item(self, tender, item, item_name, code):
        self.filtered_tender_ids_queue.get()
        logger.info(u'Tender {} bid {} {} {} identifier id {} is not valid.'.format(
            tender['id'], item_name, item_id(item), item['id'], code),
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_NOT_PROCESS},
                                  params=journal_item_params(tender, item)))

    def process_qualification(self, response, tender, qualification):
        appropriate_bid = [b for b in tender['bids'] if b['id'] == qualification['bidID']][0]
        code = appropriate_bid['tenderers'][0]['identifier']['id']
        if is_code_invalid(code):
            self.remove_invalid_item(tender, qualification, "qualification", code)
        elif self.should_process_qualification(appropriate_bid, tender, qualification):
            self.temp_important_part_for_item(response, tender, qualification, "qualification", code)
        else:
            logger.info('Tender {} bid {} qualification {} identifier schema is not UA-EDR or '
                        'tender is already in process or was processed.'.format(tender['id'], qualification['bidID'],
                                                                                qualification['id']),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_NOT_PROCESS},
                                              params=journal_item_params(tender, qualification)))

    def should_process_award(self, supplier, tender, award):
        return (supplier['identifier']['scheme'] == identification_scheme and
                check_related_lot_status(tender, award)
                and not self.process_tracker.check_processing_item(tender['id'], award['id'])
                and not self.process_tracker.check_processed_item(tender['id'], award['id']))

    def should_process_qualification(self, bid, tender, qualification):
        return (bid['tenderers'][0]['identifier']['scheme'] == identification_scheme
                and not self.process_tracker.check_processing_item(tender['id'], qualification['id'])
                and not self.process_tracker.check_processed_item(tender['id'], qualification['id']))

    def _start_jobs(self):
        return {'prepare_data': spawn(self.prepare_data)}
