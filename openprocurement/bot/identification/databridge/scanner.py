# -*- coding: utf-8 -*-
import logging.config

from datetime import datetime

import gevent
from gevent import spawn
from gevent.event import Event
from openprocurement.bot.identification.databridge.base_worker import BaseWorker
from openprocurement.bot.identification.databridge.constants import retry_mult
from openprocurement.bot.identification.databridge.journal_msg_ids import DATABRIDGE_INFO, DATABRIDGE_SYNC_SLEEP, \
    DATABRIDGE_TENDER_PROCESS, DATABRIDGE_WORKER_DIED
from openprocurement.bot.identification.databridge.utils import journal_context, generate_req_id, \
    more_tenders, valid_qualification_tender, valid_prequal_tender
from restkit import ResourceError
from retrying import retry

logger = logging.getLogger(__name__)


class Scanner(BaseWorker):
    """ Edr API Data Bridge """

    def __init__(self, tenders_sync_client, filtered_tender_ids_queue, services_not_available, process_tracker,
                 sleep_change_value, delay=15):
        super(Scanner, self).__init__(services_not_available)
        self.start_time = datetime.now()
        self.delay = delay
        # init clients
        self.tenders_sync_client = tenders_sync_client

        # init queues for workers
        self.filtered_tender_ids_queue = filtered_tender_ids_queue

        self.process_tracker = process_tracker

        # blockers
        self.initialization_event = Event()
        self.sleep_change_value = sleep_change_value

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=retry_mult)
    def initialize_sync(self, params=None, direction=None):
        if direction == "backward":
            self.initialization_event.clear()
            assert params['descending']
            response = self.tenders_sync_client.sync_tenders(params,
                                                             extra_headers={'X-Client-Request-ID': generate_req_id()})
            # set values in reverse order due to 'descending' option
            self.initial_sync_point = {'forward_offset': response.prev_page.offset,
                                       'backward_offset': response.next_page.offset}
            self.initialization_event.set()  # wake up forward worker
            logger.info("Initial sync point {}".format(self.initial_sync_point))
            return response
        else:
            assert 'descending' not in params
            self.initialization_event.wait()
            params['offset'] = self.initial_sync_point['forward_offset']
            logger.info("Starting forward sync from offset {}".format(params['offset']))
            return self.tenders_sync_client.sync_tenders(params,
                                                         extra_headers={'X-Client-Request-ID': generate_req_id()})

    def get_tenders(self, params={}, direction=""):
        response = self.initialize_sync(params=params, direction=direction)

        while more_tenders(params, response):
            tenders = response.data if response else []
            params['offset'] = response.next_page.offset
            for tender in tenders:
                if self.should_process_tender(tender):
                    yield tender
                else:
                    logger.info('Skipping tender {} with status {} with procurementMethodType {}'.format(
                        tender['id'], tender['status'], tender['procurementMethodType']),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_INFO},
                                              params={"TENDER_ID": tender['id']}))
            logger.info('Sleep {} sync...'.format(direction),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_SYNC_SLEEP}))
            gevent.sleep(self.delay + self.sleep_change_value.time_between_requests)
            try:
                response = self.tenders_sync_client.sync_tenders(params, extra_headers={
                    'X-Client-Request-ID': generate_req_id()})
                self.sleep_change_value.decrement()
            except ResourceError as re:
                if re.status_int == 429:
                    self.sleep_change_value.increment()
                    logger.info("Received 429, will sleep for {}".format(self.sleep_change_value.time_between_requests))
                else:
                    raise re

    def should_process_tender(self, tender):
        return (not self.process_tracker.check_processed_tenders(tender['id']) and
                (valid_qualification_tender(tender) or valid_prequal_tender(tender)))

    def get_tenders_forward(self):
        self.services_not_available.wait()
        logger.info('Start forward data sync worker...')
        params = {'opt_fields': 'status,procurementMethodType', 'mode': '_all_'}
        try:
            self.put_tenders_to_process(params, "forward")
        except Exception as e:
            logger.warning('Forward worker died!', extra=journal_context({"MESSAGE_ID": DATABRIDGE_WORKER_DIED}, {}))
            logger.exception("Message: {}".format(e.message))
        else:
            logger.warning('Forward data sync finished!')

    def get_tenders_backward(self):
        self.services_not_available.wait()
        logger.info('Start backward data sync worker...')
        params = {'opt_fields': 'status,procurementMethodType', 'descending': 1, 'mode': '_all_'}
        try:
            self.put_tenders_to_process(params, "backward")
        except Exception as e:
            logger.warning('Backward worker died!', extra=journal_context({"MESSAGE_ID": DATABRIDGE_WORKER_DIED}, {}))
            logger.exception("Message: {}".format(e.message))
            return False
        else:
            logger.info('Backward data sync finished.')
            return True

    def put_tenders_to_process(self, params, direction):
        for tender in self.get_tenders(params=params, direction=direction):
            logger.info('Backward sync: Put tender {} to process...'.format(tender['id']),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS},
                                              {"TENDER_ID": tender['id']}))
            self.filtered_tender_ids_queue.put(tender['id'])

    def _start_jobs(self):
        return {'get_tenders_backward': spawn(self.get_tenders_backward),
                'get_tenders_forward': spawn(self.get_tenders_forward)}

    def check_and_revive_jobs(self):
        for name, job in self.immortal_jobs.items():
            if job.dead and not job.value:
                self.revive_job(name)
