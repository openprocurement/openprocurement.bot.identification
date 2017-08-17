# -*- coding: utf-8 -*-
from gevent import monkey
from gevent.queue import Queue
from retrying import retry
from simplejson import JSONDecodeError
monkey.patch_all()

import logging.config
import gevent
from datetime import datetime
from gevent import Greenlet, spawn
from gevent.hub import LoopExit
from copy import deepcopy

from openprocurement.bot.identification.databridge.journal_msg_ids import (
    DATABRIDGE_GET_TENDER_FROM_QUEUE, DATABRIDGE_START_EDR_HANDLER, DATABRIDGE_SUCCESS_CREATE_FILE,
    DATABRIDGE_EMPTY_RESPONSE
)
from openprocurement.bot.identification.databridge.utils import (
    Data, journal_context, RetryException, check_add_suffix
)
from openprocurement.bot.identification.databridge.constants import version, retry_mult

logger = logging.getLogger(__name__)


class EdrHandler(Greenlet):
    """ Edr API Data Bridge """
    identification_scheme = u"UA-EDR"
    activityKind_scheme = u'КВЕД'

    def __init__(self, proxyClient, edrpou_codes_queue, upload_to_doc_service_queue, process_tracker, services_not_available, delay=15):
        super(EdrHandler, self).__init__()
        self.exit = False
        self.start_time = datetime.now()

        # init clients
        self.proxyClient = proxyClient

        # init queues for workers
        self.edrpou_codes_queue = edrpou_codes_queue
        self.upload_to_doc_service_queue = upload_to_doc_service_queue

        # retry queues for workers
        self.retry_edrpou_codes_queue = Queue(maxsize=500)
        self.retry_edr_ids_queue = Queue(maxsize=500)

        # blockers
        self.until_too_many_requests_event = gevent.event.Event()
        self.services_not_available = services_not_available

        self.until_too_many_requests_event.set()

        self.delay = delay
        self.process_tracker = process_tracker

    def get_edr_data(self):
        """Get data from edrpou_codes_queue; make request to EDR Api, passing EDRPOU (IPN, passport); Received data put
        into upload_to_doc_service_queue"""
        while not self.exit:
            self.services_not_available.wait()
            try:
                tender_data = self.edrpou_codes_queue.peek()
            except LoopExit:
                gevent.sleep()
                continue
            logger.info('Get {} from edrpou_codes_queue'.format(tender_data),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                              tender_data.log_params()))
            self.until_too_many_requests_event.wait()
            response = self.proxyClient.verify(tender_data.param(), tender_data.code,
                                               headers={'X-Client-Request-ID': tender_data.doc_id()})
            tender_data.add_unique_req_id(response)
            try:
                res_json = response.json()
            except JSONDecodeError:
                res_json = response.text
            if self.is_no_document_in_edr(response, res_json):
                self.move_data_nonexistent_edr(response.json(), tender_data, False)
            elif response.status_code == 200:
                self.move_data_existing_edr(response, tender_data, False)
            else:
                self.handle_status_response(response, tender_data.tender_id)
                self.retry_edrpou_codes_queue.put(tender_data)  # Put tender to retry
                logger.info('Put {} to retry_edrpou_codes_queue'.format(tender_data),
                            extra=journal_context(params=tender_data.log_params()))
            self.edrpou_codes_queue.get()
            gevent.sleep()

    def is_no_document_in_edr(self, response, res_json):
        return (response.status_code == 404 and isinstance(res_json, dict)
                and res_json.get('errors')[0].get('description')[0].get('error').get('code') == u"notFound")

    def move_data_nonexistent_edr(self, res_json, tender_data, is_retry):
        logger.info('Empty response for {} doc_id {}.'.format(tender_data, tender_data.doc_id()),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_EMPTY_RESPONSE}, tender_data.log_params()))
        file_content = res_json.get('errors')[0].get('description')[0]
        file_content['meta'].update(tender_data.file_content['meta'])
        file_content['meta'].update({"version": version})
        data = tender_data
        data.file_content = file_content
        self.process_tracker.set_item(tender_data.tender_id, tender_data.item_id, 1)
        self.upload_to_doc_service_queue.put(data)
        if is_retry:
            self.retry_edrpou_codes_queue.get()

    def move_data_existing_edr(self, response, tender_data, is_retry):
        data_list = []
        try:
            self.fill_data_list(response, tender_data, data_list)
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
            try:
                tender_data = self.retry_edrpou_codes_queue.peek()
            except LoopExit:
                gevent.sleep()
                continue
            logger.info('Get {} from retry_edrpou_codes_queue'.format(tender_data),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                              tender_data.log_params()))
            self.until_too_many_requests_event.wait()
            try:
                response = self.get_edr_data_request(tender_data.param(), tender_data.code, tender_data.doc_id())
                tender_data.add_unique_req_id(response)
            except RetryException as re:
                try:
                    self.handle_status_response(re.args[1], tender_data.tender_id)
                    res_json = re.args[1].json()
                except JSONDecodeError:
                    res_json = re.args[1].text
                if self.is_no_document_in_edr(re.args[1], res_json):
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
            gevent.sleep()

    def fill_data_list(self, response, tender_data, data_list):
        for i, obj in enumerate(response.json()['data']):
            document_id = check_add_suffix(response.json()['data'], tender_data.doc_id(), i + 1)
            file_content = {'meta': {'sourceDate': response.json()['meta']['detailsSourceDate'][i]},
                            'data': obj}
            file_content['meta'].update(deepcopy(tender_data.file_content['meta']))
            file_content['meta'].update({"version": version})  # add filed meta.version
            file_content['meta']['id'] = document_id
            data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                        tender_data.item_name, file_content)
            data_list.append(data)

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=retry_mult)
    def get_edr_data_request(self, param, code, document_id):
        """Execute request to EDR Api for retry queue objects."""
        self.until_too_many_requests_event.wait()
        response = self.proxyClient.verify(param, code, headers={'X-Client-Request-ID': document_id})
        if response.status_code not in (200, 429):
            logger.info(
                'Get unsuccessful response {} in get_edr_id_request code={} document_id={}, header {}'.format(
                    response.status_code, code, document_id, response.headers.get('X-Request-ID')))
            raise RetryException('Unsuccessful retry request to EDR.', response)
        return response

    def handle_status_response(self, response, tender_id):
        """Process unsuccessful request"""
        if response.status_code == 429:
            seconds_to_wait = response.headers.get('Retry-After', self.delay)
            logger.info('Too many requests to EDR API. Msg: {}, wait {} seconds.'.format(response.text, seconds_to_wait),
                        extra=journal_context(params={"TENDER_ID": tender_id}))
            self.wait_until_too_many_requests(seconds_to_wait)
        elif self.is_payment_required(response):
            logger.warning('Payment required for requesting info to EDR. Message: {err}'.format(err=response.text),
                           extra=journal_context(params={"TENDER_ID": tender_id}))
        else:
            logger.warning('Error appeared while requesting to EDR. Description: {err}'.format(err=response.text),
                           extra=journal_context(params={"TENDER_ID": tender_id}))

    def is_payment_required(self, response):
        return (response.status_code == 403 and response.headers.get('content-type', '') == 'application/json'
                and (response.json().get('errors')[0].get('description') ==
                     [{'message': 'Payment required.', 'code': 5}]))

    def wait_until_too_many_requests(self, seconds_to_wait):
        if self.until_too_many_requests_event.ready():
            logger.info('Bot is waiting...')
            self.until_too_many_requests_event.clear()
            self.until_too_many_requests_event.wait(float(seconds_to_wait))
            logger.info('Bot stop waiting...')
            self.until_too_many_requests_event.set()

    def check_and_revive_jobs(self):
        for name, job in self.immortal_jobs.items():
            if job.dead:
                logger.warning("EDR handler worker {} dead try restart".format(name),
                               extra=journal_context({"MESSAGE_ID": "DATABRIDGE_RESTART_{}".format(name.lower())}, {}))
                self.immortal_jobs[name] = gevent.spawn(getattr(self, name))
                logger.info("EDR handler worker {} is up".format(name))

    def _run(self):
        logger.info('Start EDR Handler', extra=journal_context({"MESSAGE_ID": DATABRIDGE_START_EDR_HANDLER}, {}))
        self.immortal_jobs = {'get_edr_data': spawn(self.get_edr_data),
                              'retry_get_edr_data': spawn(self.retry_get_edr_data)}
        try:
            while not self.exit:
                gevent.sleep(self.delay)
                self.check_and_revive_jobs()
        except Exception as e:
            logger.error(e)
            gevent.killall(self.immortal_jobs.values(), timeout=5)

    def shutdown(self):
        self.exit = True
        logger.info('Worker EDR Handler complete his job.')
