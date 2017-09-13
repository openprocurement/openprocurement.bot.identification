# -*- coding: utf-8 -*-

from gevent import monkey, event, spawn

monkey.patch_all()

import logging.config
import gevent

from datetime import datetime
from gevent.hub import LoopExit
from gevent.queue import Queue
from retrying import retry
from copy import deepcopy, copy

from openprocurement.bot.identification.databridge.base_worker import BaseWorker
from openprocurement.bot.identification.databridge.journal_msg_ids import (
    DATABRIDGE_GET_TENDER_FROM_QUEUE, DATABRIDGE_EMPTY_RESPONSE
)
from openprocurement.bot.identification.databridge.utils import (
    journal_context, RetryException, get_res_json, is_no_document_in_edr, fill_data_list, is_payment_required
)
from openprocurement.bot.identification.databridge.constants import version, retry_mult

logger = logging.getLogger(__name__)


class EdrHandler(BaseWorker):
    """ Edr API Data Bridge """
    identification_scheme = u"UA-EDR"
    activityKind_scheme = u'КВЕД'

    def __init__(self, proxy_client, edrpou_codes_queue, upload_to_doc_service_queue, process_tracker,
                 services_not_available, delay=15):
        super(EdrHandler, self).__init__(services_not_available)
        self.start_time = datetime.now()

        # init clients
        self.proxy_client = proxy_client

        # init queues for workers
        self.edrpou_codes_queue = edrpou_codes_queue
        self.upload_to_doc_service_queue = upload_to_doc_service_queue

        # retry queues for workers
        self.retry_edrpou_codes_queue = Queue(maxsize=500)
        self.retry_edr_ids_queue = Queue(maxsize=500)

        # blockers
        self.until_too_many_requests_event = event.Event()

        self.until_too_many_requests_event.set()

        self.delay = delay
        self.process_tracker = process_tracker

    def get_edr_data(self):
        """Get data from edrpou_codes_queue; make request to EDR Api, passing EDRPOU (IPN, passport); Received data put
        into upload_to_doc_service_queue"""
        while not self.exit:
            self.services_not_available.wait()
            self.try_peek_and_get_edr_data()
            gevent.sleep()

    def try_peek_and_get_edr_data(self):
        try:
            tender_data = self.edrpou_codes_queue.peek()
        except LoopExit:
            gevent.sleep()
        else:
            self.get_data_and_move_to_upload_or_retry(tender_data)

    def get_data_and_move_to_upload_or_retry(self, tender_data):
        logger.info('Get {} from edrpou_codes_queue'.format(tender_data),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                          tender_data.log_params()))
        self.until_too_many_requests_event.wait()
        response = self.proxy_client.verify(tender_data.param(), tender_data.code,
                                            headers={'X-Client-Request-ID': tender_data.doc_id()})
        tender_data.add_unique_req_id(response)
        res_json = get_res_json(response)
        if is_no_document_in_edr(response, res_json):
            self.move_data_nonexistent_edr(response.json(), tender_data, False)
        elif response.status_code == 200:
            self.move_data_existing_edr(response, tender_data, False)
        else:
            self.handle_status_response(response, tender_data.tender_id)
            self.retry_edrpou_codes_queue.put(tender_data)
            logger.info('Put {} to retry_edrpou_codes_queue'.format(tender_data),
                        extra=journal_context(params=tender_data.log_params()))
        self.edrpou_codes_queue.get()

    def move_data_nonexistent_edr(self, res_json, tender_data, is_retry):
        logger.info('Empty response for {} doc_id {}.'.format(tender_data, tender_data.doc_id()),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_EMPTY_RESPONSE}, tender_data.log_params()))
        file_content = res_json.get('errors')[0].get('description')[0]
        file_content['meta'].update(deepcopy(tender_data.file_content['meta']))
        file_content['meta'].update({"version": version})
        data = copy(tender_data)
        data.file_content = file_content
        self.process_tracker.set_item(tender_data.tender_id, tender_data.item_id, 1)
        self.process_tracker.add_unprocessed_item(tender_data)
        self.upload_to_doc_service_queue.put(data)
        if is_retry:
            self.retry_edrpou_codes_queue.get()

    def move_data_existing_edr(self, response, tender_data, is_retry):
        data_list = []
        try:
            fill_data_list(response, tender_data, data_list, self.process_tracker)
        except (KeyError, IndexError) as e:
            logger.info('Error {}. {}'.format(tender_data, e))
            self.retry_edrpou_codes_queue.put(self.retry_edrpou_codes_queue.get() if is_retry else tender_data)
        else:
            for data in data_list:
                self.upload_to_doc_service_queue.put(data)
                logger.info('Put tender {} doc_id {} to upload_to_doc_service_queue.'.format(data, data.doc_id()))
            if is_retry:
                self.retry_edrpou_codes_queue.get()
            self.process_tracker.set_item(tender_data.tender_id, tender_data.item_id, len(response.json()['data']))

    def retry_get_edr_data(self):
        """Get data from retry_edrpou_codes_queue; Put data into upload_to_doc_service_queue if request is successful,
        otherwise put data back to retry_edrpou_codes_queue."""
        while not self.exit:
            self.services_not_available.wait()
            self.try_get_retry_data_and_process()
            gevent.sleep()

    def try_get_retry_data_and_process(self):
        try:
            tender_data = self.retry_edrpou_codes_queue.peek()
        except LoopExit:
            gevent.sleep()
        else:
            self.retry_process_tender_data(tender_data)

    def retry_process_tender_data(self, tender_data):
        logger.info('Get {} from retry_edrpou_codes_queue'.format(tender_data),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                          tender_data.log_params()))
        self.retry_try_get_edr_data(tender_data)
        self.until_too_many_requests_event.wait()

    def retry_try_get_edr_data(self, tender_data):
        try:
            response = self.get_edr_data_request(tender_data.param(), tender_data.code, tender_data.doc_id())
            tender_data.add_unique_req_id(response)
        except RetryException as re:
            self.handle_status_response(re.args[1], tender_data.tender_id)
            res_json = get_res_json(re.args[1])
            if is_no_document_in_edr(re.args[1], res_json):
                self.move_data_nonexistent_edr(res_json, tender_data, True)
            else:
                logger.info('Put {} in back of retry_edrpou_codes_queue. Response {}'.format(tender_data, res_json),
                            extra=journal_context(params=tender_data.log_params()))
                self.retry_edrpou_codes_queue.put(self.retry_edrpou_codes_queue.get())
                gevent.sleep()
        except Exception as e:
            logger.info('Put {} in back of retry_edrpou_codes_queue. Error: {}'.format(tender_data, e.message),
                        extra=journal_context(params=tender_data.log_params()))
            self.retry_edrpou_codes_queue.put(self.retry_edrpou_codes_queue.get())
            gevent.sleep()
        else:
            if response.status_code == 429:
                seconds_to_wait = response.headers.get('Retry-After', self.delay)
                logger.info('retry_get_edr_id: Too many requests to EDR API. Msg: {}, wait {} seconds.'.format(
                    response.text, seconds_to_wait), extra=journal_context(params=tender_data.log_params()))
                self.wait_until_too_many_requests(seconds_to_wait)
            elif response.status_code == 200:
                self.move_data_existing_edr(response, tender_data, True)

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=retry_mult)
    def get_edr_data_request(self, param, code, document_id):
        """Execute request to EDR Api for retry queue objects."""
        self.until_too_many_requests_event.wait()
        response = self.proxy_client.verify(param, code, headers={'X-Client-Request-ID': document_id})
        if response.status_code not in (200, 429):
            logger.info('Get unsuccessful response {} code={} document_id={}, header {}'.format(
                response.status_code, code, document_id, response.headers.get('X-Request-ID')))
            raise RetryException('Unsuccessful retry request to EDR.', response)
        return response

    def handle_status_response(self, response, tender_id):
        """Process unsuccessful request"""
        if response.status_code == 429:
            seconds_to_wait = response.headers.get('Retry-After', self.delay)
            logger.info('Too many requests to EDR API. Msg: {}, wait {} seconds.'.format(
                response.text, seconds_to_wait), extra=journal_context(params={"TENDER_ID": tender_id}))
            self.wait_until_too_many_requests(seconds_to_wait)
        elif is_payment_required(response):
            logger.warning('Payment required for requesting info to EDR. Message: {err}'.format(err=response.text),
                           extra=journal_context(params={"TENDER_ID": tender_id}))
        else:
            logger.warning('Error appeared while requesting to EDR. Description: {err}'.format(err=response.text),
                           extra=journal_context(params={"TENDER_ID": tender_id}))

    def wait_until_too_many_requests(self, seconds_to_wait):
        if self.until_too_many_requests_event.ready():
            logger.info('Bot is waiting...')
            self.until_too_many_requests_event.clear()
            self.until_too_many_requests_event.wait(float(seconds_to_wait))
            logger.info('Bot stop waiting...')
            self.until_too_many_requests_event.set()

    def _start_jobs(self):
        return {'get_edr_data': spawn(self.get_edr_data),
                'retry_get_edr_data': spawn(self.retry_get_edr_data)}
