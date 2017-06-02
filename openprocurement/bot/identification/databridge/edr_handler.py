# -*- coding: utf-8 -*-
from gevent import monkey
from gevent.queue import Queue
from retrying import retry
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
    Data, journal_context, validate_param, RetryException, check_add_suffix, data_string
)
from openprocurement.bot.identification.databridge.constants import version

logger = logging.getLogger(__name__)


class EdrHandler(Greenlet):
    """ Edr API Data Bridge """
    identification_scheme = u"UA-EDR"
    activityKind_scheme = u'КВЕД'

    def __init__(self, proxyClient, edrpou_codes_queue, upload_to_doc_service_queue, processing_items, delay=15):
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

        self.until_too_many_requests_event.set()

        self.delay = delay
        self.processing_items = processing_items

    def get_edr_data(self):
        """Get data from edrpou_codes_queue; make request to EDR Api, passing EDRPOU (IPN, passport); Received data put
        into upload_to_doc_service_queue"""
        while not self.exit:
            try:
                tender_data = self.edrpou_codes_queue.peek()
            except LoopExit:
                gevent.sleep(0)
                continue
            item_name_id = tender_data.item_name[:-1].upper() + "_ID"
            logger.info('Get {} from edrpou_codes_queue'.format(data_string(tender_data)),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                              params={"TENDER_ID": tender_data.tender_id, item_name_id: tender_data.item_id}))
            self.until_too_many_requests_event.wait()
            document_id = tender_data.file_content['meta']['id']
            response = self.proxyClient.verify(validate_param(tender_data.code), tender_data.code, headers={'X-Client-Request-ID': document_id})
            if response.status_code == 404 and response.json().get('errors')[0].get('description')[0].get('error').get('code') == u"notFound":
                logger.info('Empty response for {} doc_id {}.'.format(data_string(tender_data), document_id),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_EMPTY_RESPONSE},
                                                  params={"TENDER_ID": tender_data.tender_id, item_name_id: tender_data.item_id, "DOCUMENT_ID": document_id}))
                file_content = response.json().get('errors')[0].get('description')[0]
                file_content['meta'].update(tender_data.file_content['meta'])  # add meta.id to file_content
                file_content['meta'].update({"version": version})  # add filed meta.version
                if response.headers.get('X-Request-ID'):
                    file_content['meta']['sourceRequests'].append(response.headers['X-Request-ID'])  # add unique request id
                data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code, tender_data.item_name, file_content)
                self.upload_to_doc_service_queue.put(data)
                self.edrpou_codes_queue.get()
                continue
            if response.status_code == 200:
                meta_id = tender_data.file_content['meta']['id']
                for obj in response.json():
                    try:
                        document_id = check_add_suffix(response.json(), meta_id, response.json().index(obj) + 1)
                        file_content = obj
                        file_content['meta'].update(deepcopy(tender_data.file_content['meta']))
                        file_content['meta'].update({"version": version})  # add filed meta.version
                        file_content['meta']['sourceRequests'].append(response.headers['X-Request-ID'])
                        file_content['meta']['id'] = document_id
                        data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                    tender_data.item_name, obj)
                    except KeyError as e:
                        logger.info('Error data type {}. {}'.format(data_string(tender_data), e))
                        self.retry_edrpou_codes_queue.put(tender_data)
                    else:
                        self.upload_to_doc_service_queue.put(data)
                        logger.info('Put tender {} to upload_to_doc_service_queue.'.format(data_string(tender_data)))
                self.processing_items['{}_{}'.format(tender_data.tender_id, tender_data.item_id)] = len(response.json())
            else:
                if response.headers.get('X-Request-ID'):
                    tender_data.file_content['meta']['sourceRequests'].append(response.headers['X-Request-ID'])
                self.handle_status_response(response, tender_data.tender_id)
                self.retry_edrpou_codes_queue.put(tender_data)  # Put tender to retry
                logger.info('Put {} to retry_edrpou_codes_queue'.format(data_string(tender_data)),
                            extra=journal_context(params={"TENDER_ID": tender_data.tender_id, item_name_id: tender_data.item_id}))
            self.edrpou_codes_queue.get()
            gevent.sleep(0)

    def retry_get_edr_data(self):
        """Get data from retry_edrpou_codes_queue; Put data into upload_to_doc_service_queue if request is successful,
        otherwise put data back to retry_edrpou_codes_queue."""
        while not self.exit:
            try:
                tender_data = self.retry_edrpou_codes_queue.peek()
            except LoopExit:
                gevent.sleep(0)
                continue
            item_name_id = tender_data.item_name[:-1].upper() + "_ID"
            logger.info('Get {} from retry_edrpou_codes_queue'.format(data_string(tender_data)),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                              params={"TENDER_ID": tender_data.tender_id, item_name_id: tender_data.item_id}))
            self.until_too_many_requests_event.wait()
            document_id = tender_data.file_content['meta']['id']
            try:
                response = self.get_edr_data_request(validate_param(tender_data.code), tender_data.code, document_id)
                if response.headers.get('X-Request-ID'):
                    tender_data.file_content['meta']['sourceRequests'].append(response.headers['X-Request-ID'])
            except RetryException as re:
                if re.args[1].status_code == 404 and re.args[1].json().get('errors')[0].get('description')[0].get('error').get('code') == u"notFound":
                    logger.info('Empty response for {} doc_id: {}.'.format(data_string(tender_data), document_id),
                                extra=journal_context({"MESSAGE_ID": DATABRIDGE_EMPTY_RESPONSE},
                                                      params={"TENDER_ID": tender_data.tender_id, item_name_id: tender_data.item_id, "DOCUMENT_ID": document_id}))
                    file_content = re.args[1].json().get('errors')[0].get('description')[0]
                    file_content['meta'].update(tender_data.file_content['meta'])
                    file_content['meta'].update({"version": version})  # add filed meta.version
                    data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                tender_data.item_name, file_content)
                    self.upload_to_doc_service_queue.put(data)  # Given EDRPOU code not found, file with error put into upload_to_doc_service_queue
                    self.retry_edrpou_codes_queue.get()
                    continue
                logger.info("RetryException error message {}".format(re.args[0]))
                self.handle_status_response(re.args[1], tender_data.tender_id)
                logger.info('Put {} in back of retry_edrpou_codes_queue'.format(data_string(tender_data)),
                            extra=journal_context(params={"TENDER_ID": tender_data.tender_id, item_name_id: tender_data.item_id}))
                self.retry_edrpou_codes_queue.put(self.retry_edrpou_codes_queue.get())
                gevent.sleep(0)
            except Exception:
                logger.info('Put {} in back of retry_edrpou_codes_queue'.format(data_string(tender_data)),
                            extra=journal_context(params={"TENDER_ID": tender_data.tender_id, item_name_id: tender_data.item_id}))
                self.retry_edrpou_codes_queue.put(self.retry_edrpou_codes_queue.get())
                gevent.sleep(0)
            else:
                # Create new Data object. Write to Data.code list of edr ids from EDR.
                # List because EDR can return 0, 1 or 2 values to our request
                if response.status_code == 429:
                    seconds_to_wait = response.headers.get('Retry-After', self.delay)
                    logger.info('retry_get_edr_id: Too many requests to EDR API. Msg: {}, wait {} seconds.'.format(response.text,
                                                                                                 seconds_to_wait),
                                extra=journal_context(params={"TENDER_ID": tender_data.tender_id, item_name_id: tender_data.item_id}))
                    self.wait_until_too_many_requests(seconds_to_wait)
                    continue
                if response.status_code == 200:
                    for obj in response.json():
                        try:
                            file_content = obj
                            file_content['meta'].update(tender_data.file_content['meta'])
                            file_content['meta'].update({"version": version})  # add filed meta.version
                            data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                        tender_data.item_name, file_content)
                        except KeyError as e:
                            logger.info('Error data type {}. {}'.format(data_string(tender_data), e))
                            self.retry_edrpou_codes_queue.put(self.retry_edrpou_codes_queue.get())
                        else:
                            self.upload_to_doc_service_queue.put(data)
                            self.retry_edrpou_codes_queue.get()
                            logger.info('Put tender {} in retry to '
                                        'upload_to_doc_service_queue'.format(data_string(tender_data)))
                    self.processing_items['{}_{}'.format(tender_data.tender_id, tender_data.item_id)] = len(response.json())
            gevent.sleep(0)

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
    def get_edr_data_request(self, param, code, document_id):
        """Execute request to EDR Api for retry queue objects."""
        self.until_too_many_requests_event.wait()
        response = self.proxyClient.verify(param, code, headers={'X-Client-Request-ID': document_id})
        if response.status_code not in (200, 429):
            logger.info(
                'Get unsuccessful response {} in get_edr_id_request code={} document_id={}, header {}'.format(response.status_code, param, code, response.headers.get('X-Request-ID')))
            raise RetryException('Unsuccessful retry request to EDR.', response)
        return response

    def handle_status_response(self, response, tender_id):
        """Process unsuccessful request"""
        if response.status_code == 429:
            seconds_to_wait = response.headers.get('Retry-After', self.delay)
            logger.info('Too many requests to EDR API. Msg: {}, wait {} seconds.'.format(response.text, seconds_to_wait),
                        extra=journal_context(params={"TENDER_ID": tender_id}))
            self.wait_until_too_many_requests(seconds_to_wait)
        elif response.status_code == 403 and response.json().get('errors')[0].get('description') == [{'message': 'Payment required.', 'code': 5}]:
            logger.warning('Payment required for requesting info to EDR. '
                           'Error description: {err}'.format(err=response.text),
                           extra=journal_context(params={"TENDER_ID": tender_id}))
        else:
            logger.warning('Error appeared while requesting to EDR. '
                           'Description: {err}'.format(err=response.text),
                           extra=journal_context(params={"TENDER_ID": tender_id}))

    def wait_until_too_many_requests(self, seconds_to_wait):
        if self.until_too_many_requests_event.ready():
            logger.info('Bot is waiting...')
            self.until_too_many_requests_event.clear()
            self.until_too_many_requests_event.wait(float(seconds_to_wait))
            self.until_too_many_requests_event.set()
            logger.info('Bot stop waiting...')

    def _run(self):
        logger.info('Start EDR Handler', extra=journal_context({"MESSAGE_ID": DATABRIDGE_START_EDR_HANDLER}, {}))
        self.immortal_jobs = {'get_edr_data': spawn(self.get_edr_data),
                              'retry_get_edr_data': spawn(self.retry_get_edr_data)}

        try:
            while not self.exit:
                gevent.sleep(self.delay)
                for name, job in self.immortal_jobs.items():
                    if job.dead:
                        logger.warning("EDR handler worker {} dead try restart".format(name),
                                       extra=journal_context({"MESSAGE_ID": "DATABRIDGE_RESTART_{}".format(name.lower())}, {}))
                        self.immortal_jobs[name] = gevent.spawn(getattr(self, name))
                        logger.info("EDR handler worker {} is up".format(name))
        except Exception as e:
            logger.error(e)
            gevent.killall(self.immortal_jobs.values(), timeout=5)

    def shutdown(self):
        self.exit = True
        logger.info('Worker EDR Handler complete his job.')
