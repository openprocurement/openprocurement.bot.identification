# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import logging.config
import gevent
from datetime import datetime
from gevent import Greenlet, spawn
from gevent.hub import LoopExit
from munch import munchify
from simplejson import loads

from openprocurement.bot.identification.databridge.utils import (
    generate_req_id, journal_context, Data, generate_doc_id
)
from openprocurement.bot.identification.databridge.journal_msg_ids import (
    DATABRIDGE_GET_TENDER_FROM_QUEUE, DATABRIDGE_TENDER_PROCESS,
    DATABRIDGE_START_FILTER_TENDER, DATABRIDGE_RESTART_FILTER_TENDER,
    DATABRIDGE_TENDER_NOT_PROCESS
)
from openprocurement.bot.identification.databridge.constants import author
from restkit import ResourceError


logger = logging.getLogger(__name__)


class FilterTenders(Greenlet):
    """ Edr API Data Bridge """
    identification_scheme = u'UA-EDR'
    sleep_change_value = 0

    def __init__(self, tenders_sync_client, filtered_tender_ids_queue, edrpou_codes_queue, processing_items, increment_step=1, decrement_step=1,  delay=15):
        super(FilterTenders, self).__init__()
        self.exit = False
        self.start_time = datetime.now()

        self.delay = delay
        # init clients
        self.tenders_sync_client = tenders_sync_client

        # init queues for workers
        self.filtered_tender_ids_queue = filtered_tender_ids_queue
        self.edrpou_codes_queue = edrpou_codes_queue
        self.processing_items = processing_items
        self.increment_step = increment_step
        self.decrement_step = decrement_step

    def prepare_data(self):
        """Get tender_id from filtered_tender_ids_queue, check award/qualification status, documentType; get
        identifier's id and put into edrpou_codes_queue."""
        while not self.exit:
            try:
                tender_id = self.filtered_tender_ids_queue.peek()
            except LoopExit:
                gevent.sleep(0)
                continue
            try:
                response = self.tenders_sync_client.request("GET", path='{}/{}'.format(self.tenders_sync_client.prefix_path, tender_id),
                                                            headers={'X-Client-Request-ID': generate_req_id()})
            except ResourceError as re:
                if re.status_int == 429:
                    FilterTenders.sleep_change_value += self.increment_step
                    logger.info("Waiting tender {} for sleep_change_value: {} seconds".format(tender_id, FilterTenders.sleep_change_value))
                else:
                    logger.warning('Fail to get tender info {}'.format(tender_id),
                                   extra=journal_context(params={"TENDER_ID": tender_id}))
                    logger.exception("Message: {}, status_code {}".format(re.msg, re.status_int))
                    logger.info('Leave tender {} in tenders queue'.format(tender_id),
                                extra=journal_context(params={"TENDER_ID": tender_id}))
                    gevent.sleep(0)
            except Exception as e:
                logger.warning('Fail to get tender info {}'.format(tender_id),
                               extra=journal_context(params={"TENDER_ID": tender_id}))
                logger.exception("Message: {}".format(e.message))
                logger.info('Leave tender {} in tenders queue'.format(tender_id),
                            extra=journal_context(params={"TENDER_ID": tender_id}))
                gevent.sleep(0)
            else:
                FilterTenders.sleep_change_value = FilterTenders.sleep_change_value - self.decrement_step if self.decrement_step < FilterTenders.sleep_change_value else 0
                if response.status_int == 200:
                    tender = munchify(loads(response.body_string()))['data']
                logger.info('Get tender {} from filtered_tender_ids_queue'.format(tender_id),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                            params={"TENDER_ID": tender['id']}))
                if 'awards' in tender:
                    for award in tender['awards']:
                        logger.info('Processing tender {} bid {} award {}'.format(tender['id'], award['bid_id'], award['id']),
                                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS},
                                    params={"TENDER_ID": tender['id'], "BID_ID": award['bid_id'], "AWARD_ID": award['id']}))
                        if award['status'] == 'pending' and not [document for document in award.get('documents', [])
                                                                 if document.get('documentType') == 'registerExtract']:
                            for supplier in award['suppliers']:
                                code = supplier['identifier']['id']
                                if not (type(code) == int or(type(code) == str and code.isdigit()) or (type(code) == unicode and code.isdigit())):
                                    self.filtered_tender_ids_queue.get()
                                    logger.info(u'Tender {} bid {} award {} identifier id {} is not valid.'.format(
                                        tender['id'], award['bid_id'], award['id'], code),
                                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_NOT_PROCESS},
                                                              params={"TENDER_ID": tender['id'], "BID_ID": award['bid_id'], "AWARD_ID": award['id']}))
                                    continue
                                # check first identification scheme, if yes then check if item is already in process or not
                                if (supplier['identifier']['scheme'] == self.identification_scheme and
                                        self.check_related_lot_status(tender, award) and
                                        self.check_processing_item(tender['id'], award['id'])):
                                    self.processing_items['{}_{}'.format(tender['id'], award['id'])] = 0
                                    document_id = generate_doc_id()
                                    tender_data = Data(tender['id'], award['id'], str(code),
                                                       'awards', None, {'meta': {'id': document_id, 'author': author, 'sourceRequests': [response.headers['X-Request-ID']]}})
                                    self.edrpou_codes_queue.put(tender_data)
                                else:
                                    logger.info('Tender {} bid {} award {} identifier schema isn\'t UA-EDR or tender is already in process.'.format(
                                        tender['id'], award['bid_id'], award['id']),
                                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_NOT_PROCESS},
                                                              params={"TENDER_ID": tender['id'], "BID_ID":  award['bid_id'], "AWARD_ID": award['id']}))
                        else:
                            logger.info('Tender {} bid {} award {} is not in status pending or award has already document '
                                        'with documentType registerExtract.'.format(tender_id, award['bid_id'], award['id']),
                                        extra=journal_context(params={"TENDER_ID": tender['id'], "BID_ID":  award['bid_id'], "AWARD_ID": award['id']}))
                elif 'qualifications' in tender:
                    for qualification in tender['qualifications']:
                        if (qualification['status'] == 'pending' and
                                not [doc for doc in qualification.get('documents', []) if doc.get('documentType') == 'registerExtract']):
                            appropriate_bid = [b for b in tender['bids'] if b['id'] == qualification['bidID']][0]
                            code = appropriate_bid['tenderers'][0]['identifier']['id']
                            if not (type(code) == int or (type(code) == str and code.isdigit()) or (type(code) == unicode and code.isdigit())):
                                self.filtered_tender_ids_queue.get()
                                logger.info(u'Tender {} bid {} qualification {} identifier id {} is not valid.'.format(
                                    tender['id'], qualification['bidID'], qualification['id'], code),
                                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_NOT_PROCESS},
                                                          params={"TENDER_ID": tender['id'], "BID_ID":
                                                              qualification['bidID'], "QUALIFICATION_ID": qualification['id']}))
                                continue
                            # check first identification scheme, if yes then check if item is already in process or not
                            if appropriate_bid['tenderers'][0]['identifier']['scheme'] == self.identification_scheme and self.check_processing_item(tender['id'], qualification['id']):
                                self.processing_items['{}_{}'.format(tender['id'], qualification['id'])] = 0
                                document_id = generate_doc_id()
                                tender_data = Data(tender['id'], qualification['id'], str(code),
                                                   'qualifications', None, {'meta': {'id': document_id, 'author': author, 'sourceRequests': [response.headers['X-Request-ID']]}})
                                self.edrpou_codes_queue.put(tender_data)
                                logger.info('Processing tender {} bid {} qualification {}'.format(
                                    tender['id'], qualification['bidID'], qualification['id']),
                                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS},
                                                          params={"TENDER_ID": tender['id'], "BID_ID": qualification['bidID'], "QUALIFICATION_ID": qualification['id']}))
                            else:
                                logger.info('Tender {} bid {} qualification {} identifier schema is not UA-EDR or tender is already in process.'.format(
                                    tender['id'], qualification['bidID'], qualification['id']),
                                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_NOT_PROCESS},
                                                          params={"TENDER_ID": tender['id'],
                                                                  "BID_ID": qualification['bidID'], "QUALIFICATION_ID": qualification['id']}))
                        else:
                            logger.info('Tender {} bid {} qualification {} is not in status pending or qualification has '
                                        'already document with documentType registerExtract.'.format(
                                            tender_id, qualification['bidID'], qualification['id']),
                                            extra=journal_context(params={"TENDER_ID": tender['id'],
                                                                          "BID_ID": qualification['bidID'], "QUALIFICATION_ID": qualification['id']}))
                self.filtered_tender_ids_queue.get()  # Remove elem from queue
            gevent.sleep(FilterTenders.sleep_change_value)

    def check_processing_item(self, tender_id, item_id):
        """Check if current tender_id, item_id is processing"""
        return '{}_{}'.format(tender_id, item_id) not in self.processing_items.keys()

    def check_related_lot_status(self, tender, award):
        """Check if related lot not in status cancelled"""
        lot_id = award.get('lotID')
        if lot_id:
            if [l['status'] for l in tender.get('lots', []) if l['id'] == lot_id][0] != 'active':
                return False
        return True

    def _run(self):
        logger.info('Start Filter Tenders', extra=journal_context({"MESSAGE_ID": DATABRIDGE_START_FILTER_TENDER}, {}))
        self.job = spawn(self.prepare_data)

        try:
            while not self.exit:
                gevent.sleep(self.delay)
                if self.job.dead:
                    logger.warning("Filter tender job die. Try to restart.",  extra=journal_context({"MESSAGE_ID": DATABRIDGE_RESTART_FILTER_TENDER}, {}))
                    self.job = spawn(self.prepare_data)
                    logger.info("filter tenders job restarted.")
        except Exception as e:
            logger.error(e)
            self.job.kill(timeout=5)

    def shutdown(self):
        self.exit = True
        logger.info('Worker Filter Tenders complete his job.')
