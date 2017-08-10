# -*- coding: utf-8 -*-

from datetime import datetime
import logging.config
import gevent
from gevent.event import Event
from gevent import Greenlet, spawn
from openprocurement.bot.identification.databridge.constants import retry_mult, \
    pre_qualification_procurementMethodType, qualification_procurementMethodType
from retrying import retry
from restkit import ResourceError

from openprocurement.bot.identification.databridge.journal_msg_ids import DATABRIDGE_INFO, DATABRIDGE_SYNC_SLEEP, \
    DATABRIDGE_TENDER_PROCESS, DATABRIDGE_WORKER_DIED, DATABRIDGE_RESTART, DATABRIDGE_START_SCANNER
from openprocurement.bot.identification.databridge.utils import journal_context, generate_req_id

logger = logging.getLogger(__name__)


class Scanner(Greenlet):
    """ Edr API Data Bridge """

    def __init__(self, tenders_sync_client, filtered_tender_ids_queue, services_not_available, process_tracker,
                 sleep_change_value, delay=15):
        super(Scanner, self).__init__()
        self.exit = False
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
        self.services_not_available = services_not_available

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

        while not (params.get('descending') and
                       not len(response.data) and
                           params.get('offset') == response.next_page.offset):
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
                (self.valid_qualification_tender(tender) or self.valid_prequal_tender(tender)))

    def valid_qualification_tender(self, tender):
        return (tender['status'] == "active.qualification" and
                tender['procurementMethodType'] in qualification_procurementMethodType)

    def valid_prequal_tender(self, tender):
        return (tender['status'] == 'active.pre-qualification' and
                tender['procurementMethodType'] in pre_qualification_procurementMethodType)

    def get_tenders_forward(self):
        logger.info('Start forward data sync worker...')
        params = {'opt_fields': 'status,procurementMethodType', 'mode': '_all_'}
        try:
            for tender in self.get_tenders(params=params, direction="forward"):
                logger.info('Forward sync: Put tender {} to process...'.format(tender['id']),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS},
                                                  {"TENDER_ID": tender['id']}))
                self.filtered_tender_ids_queue.put(tender['id'])
        except Exception as e:
            logger.warning('Forward worker died!', extra=journal_context({"MESSAGE_ID": DATABRIDGE_WORKER_DIED}, {}))
            logger.exception("Message: {}".format(e.message))
        else:
            logger.warning('Forward data sync finished!')

    def get_tenders_backward(self):
        logger.info('Start backward data sync worker...')
        params = {'opt_fields': 'status,procurementMethodType', 'descending': 1, 'mode': '_all_'}
        try:
            for tender in self.get_tenders(params=params, direction="backward"):
                logger.info('Backward sync: Put tender {} to process...'.format(tender['id']),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS},
                                                  {"TENDER_ID": tender['id']}))
                self.filtered_tender_ids_queue.put(tender['id'])
        except Exception as e:
            logger.warning('Backward worker died!', extra=journal_context({"MESSAGE_ID": DATABRIDGE_WORKER_DIED}, {}))
            logger.exception("Message: {}".format(e.message))
            return False
        else:
            logger.info('Backward data sync finished.')
            return True

    def _start_synchronization_workers(self):
        logger.info('Scanner starting forward and backward sync workers')
        self.jobs = [spawn(self.get_tenders_backward),
                     spawn(self.get_tenders_forward)]

    def _restart_synchronization_workers(self):
        logger.warning('Restarting synchronization', extra=journal_context({"MESSAGE_ID": DATABRIDGE_RESTART}, {}))
        for j in self.jobs:
            j.kill(timeout=5)
        self._start_synchronization_workers()

    def _run(self):
        logger.info('Start Scanner', extra=journal_context({"MESSAGE_ID": DATABRIDGE_START_SCANNER}, {}))
        self.services_not_available.wait()
        self._start_synchronization_workers()
        backward_worker, forward_worker = self.jobs

        try:
            while not self.exit:
                self.services_not_available.wait()
                gevent.sleep(self.delay)
                if forward_worker.dead or (backward_worker.dead and not backward_worker.value):
                    self._restart_synchronization_workers()
                    backward_worker, forward_worker = self.jobs
        except Exception as e:
            logger.exception(e)
            raise e

    def shutdown(self):
        self.exit = True
        logger.info('Worker Scanner complete his job.')
