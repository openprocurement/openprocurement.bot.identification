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

    def __init__(self, tenders_sync_client, filtered_tender_ids_queue, edrpou_codes_queue, process_tracker,
                 services_not_available, sleep_change_value, delay=15):
        super(FilterTenders, self).__init__()
        self.exit = False
        self.start_time = datetime.now()

        self.delay = delay
        # init clients
        self.tenders_sync_client = tenders_sync_client

        # init queues for workers
        self.filtered_tender_ids_queue = filtered_tender_ids_queue
        self.edrpou_codes_queue = edrpou_codes_queue
        self.process_tracker = process_tracker
        self.sleep_change_value = sleep_change_value

        # blockers
        self.services_not_available = services_not_available

    def prepare_data(self):
        """Get tender_id from filtered_tender_ids_queue, check award/qualification status, documentType; get
        identifier's id and put into edrpou_codes_queue."""
        while not self.exit:
            self.services_not_available.wait()
            try:
                tender_id = self.filtered_tender_ids_queue.peek()
            except LoopExit:
                gevent.sleep(0)
                continue
            try:
                response = self.tenders_sync_client.request("GET",
                                                            path='{}/{}'.format(self.tenders_sync_client.prefix_path,
                                                                                tender_id),
                                                            headers={'X-Client-Request-ID': generate_req_id()})
            except ResourceError as re:
                if re.status_int == 429:
                    self.sleep_change_value.increment()
                    logger.info("Waiting tender {} for sleep_change_value: {} seconds".format(tender_id,
                                                                                              self.sleep_change_value.time_between_requests))
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
                self.sleep_change_value.decrement()
                if response.status_int == 200:
                    tender = munchify(loads(response.body_string()))['data']
                logger.info('Get tender {} from filtered_tender_ids_queue'.format(tender_id),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                                  params={"TENDER_ID": tender['id']}))
                if 'awards' in tender:
                    for award in tender['awards']:
                        logger.info(
                            'Processing tender {} bid {} award {}'.format(tender['id'], award['bid_id'], award['id']),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS},
                                                  params={"TENDER_ID": tender['id'], "BID_ID": award['bid_id'],
                                                          "AWARD_ID": award['id']}))
                        if self.should_process_item(award):
                            for supplier in award['suppliers']:
                                code = supplier['identifier']['id']
                                if self.is_code_invalid(code):
                                    self.filtered_tender_ids_queue.get()
                                    logger.info(u'Tender {} bid {} award {} identifier id {} is not valid.'.format(
                                        tender['id'], award['bid_id'], award['id'], code),
                                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_NOT_PROCESS},
                                                              params={"TENDER_ID": tender['id'],
                                                                      "BID_ID": award['bid_id'],
                                                                      "AWARD_ID": award['id']}))
                                    continue
                                    # quick check if item was already processed
                                if self.process_tracker.check_processed_item(tender['id'], award['id']):
                                    logger.info('Tender {} bid {} award {} was already processed.'.format(
                                        tender['id'], award['bid_id'], award['id']),
                                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_NOT_PROCESS},
                                                              params={"TENDER_ID": tender['id'],
                                                                      "BID_ID": award['bid_id'],
                                                                      "AWARD_ID": award['id']}))
                                elif self.should_process_award(supplier, tender, award):
                                    self.process_tracker.set_item(tender['id'], award['id'])
                                    document_id = generate_doc_id()
                                    tender_data = Data(tender['id'], award['id'], str(code),
                                                       'awards', {'meta': {'id': document_id, 'author': author,
                                                                           'sourceRequests': [
                                                                               response.headers['X-Request-ID']]}})
                                    self.edrpou_codes_queue.put(tender_data)
                                else:
                                    logger.info(
                                        'Tender {} bid {} award {} identifier schema isn\'t UA-EDR or tender is already in process.'.format(
                                            tender['id'], award['bid_id'], award['id']),
                                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_NOT_PROCESS},
                                                              params={"TENDER_ID": tender['id'],
                                                                      "BID_ID": award['bid_id'],
                                                                      "AWARD_ID": award['id']}))
                        else:
                            logger.info(
                                'Tender {} bid {} award {} is not in status pending or award has already document '
                                'with documentType registerExtract.'.format(tender_id, award['bid_id'], award['id']),
                                extra=journal_context(params={"TENDER_ID": tender['id'], "BID_ID": award['bid_id'],
                                                              "AWARD_ID": award['id']}))
                elif 'qualifications' in tender:
                    for qualification in tender['qualifications']:
                        if self.should_process_item(qualification):
                            appropriate_bid = [b for b in tender['bids'] if b['id'] == qualification['bidID']][0]
                            code = appropriate_bid['tenderers'][0]['identifier']['id']
                            if self.is_code_invalid(code):
                                self.filtered_tender_ids_queue.get()
                                logger.info(u'Tender {} bid {} qualification {} identifier id {} is not valid.'.format(
                                    tender['id'], qualification['bidID'], qualification['id'], code),
                                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_NOT_PROCESS},
                                                          params={"TENDER_ID": tender['id'], "BID_ID":
                                                              qualification['bidID'],
                                                                  "QUALIFICATION_ID": qualification['id']}))
                                continue
                            # quick check if item was already processed
                            if self.process_tracker.check_processed_item(tender['id'], qualification['id']):
                                logger.info('Tender {} bid {} qualification {} was already processed.'.format(
                                    tender['id'], qualification['bidID'], qualification['id']),
                                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_NOT_PROCESS},
                                                          params={"TENDER_ID": tender['id'],
                                                                  "BID_ID": qualification['bidID'],
                                                                  "QUALIFICATION_ID": qualification['id']}))
                            # check first identification scheme, if yes then check if item is already in process or not
                            elif self.should_process_qualification(appropriate_bid, tender, qualification):
                                self.process_tracker.set_item(tender['id'], qualification['id'])
                                document_id = generate_doc_id()
                                tender_data = Data(tender['id'], qualification['id'], str(code),
                                                   'qualifications', {'meta': {'id': document_id, 'author': author,
                                                                               'sourceRequests': [
                                                                                   response.headers['X-Request-ID']]}})
                                self.edrpou_codes_queue.put(tender_data)
                                logger.info('Processing tender {} bid {} qualification {}'.format(
                                    tender['id'], qualification['bidID'], qualification['id']),
                                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS},
                                                          params={"TENDER_ID": tender['id'],
                                                                  "BID_ID": qualification['bidID'],
                                                                  "QUALIFICATION_ID": qualification['id']}))
                            else:
                                logger.info(
                                    'Tender {} bid {} qualification {} identifier schema is not UA-EDR or tender is already in process.'.format(
                                        tender['id'], qualification['bidID'], qualification['id']),
                                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_NOT_PROCESS},
                                                          params={"TENDER_ID": tender['id'],
                                                                  "BID_ID": qualification['bidID'],
                                                                  "QUALIFICATION_ID": qualification['id']}))
                        else:
                            logger.info(
                                'Tender {} bid {} qualification {} is not in status pending or qualification has '
                                'already document with documentType registerExtract.'.format(
                                    tender_id, qualification['bidID'], qualification['id']),
                                extra=journal_context(params={"TENDER_ID": tender['id'],
                                                              "BID_ID": qualification['bidID'],
                                                              "QUALIFICATION_ID": qualification['id']}))
                self.filtered_tender_ids_queue.get()  # Remove elem from queue
            gevent.sleep(self.sleep_change_value.time_between_requests)

    def should_process_item(self, item):
        return (item['status'] == 'pending' and not [document for document in item.get('documents', [])
                                                     if document.get('documentType') == 'registerExtract'])

    def is_code_invalid(self, code):
        return (not (type(code) == int or (type(code) == str and code.isdigit()) or
                     (type(code) == unicode and code.isdigit())))

    def should_process_award(self, supplier, tender, award):
        return (supplier['identifier']['scheme'] == self.identification_scheme and
                self.check_related_lot_status(tender, award) and
                not self.process_tracker.check_processing_item(tender['id'], award['id']))

    def should_process_qualification(self, bid, tender, qualification):
        return (bid['tenderers'][0]['identifier']['scheme'] == self.identification_scheme
                and not self.process_tracker.check_processing_item(tender['id'], qualification['id']))

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
                    logger.warning("Filter tender job die. Try to restart.",
                                   extra=journal_context({"MESSAGE_ID": DATABRIDGE_RESTART_FILTER_TENDER}, {}))
                    self.job = spawn(self.prepare_data)
                    logger.info("filter tenders job restarted.")
        except Exception as e:
            logger.error(e)
            self.job.kill(timeout=5)

    def shutdown(self):
        self.exit = True
        logger.info('Worker Filter Tenders complete his job.')
