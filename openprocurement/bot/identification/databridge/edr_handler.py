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
    Data, journal_context, validate_param, RetryException, check_add_suffix
)
from openprocurement.bot.identification.databridge.constants import version

logger = logging.getLogger(__name__)


class EdrHandler(Greenlet):
    """ Edr API Data Bridge """
    identification_scheme = u"UA-EDR"
    activityKind_scheme = u'КВЕД'

    def __init__(self, proxyClient, edrpou_codes_queue, edr_ids_queue, upload_to_doc_service_queue, processing_items, delay=15):
        super(EdrHandler, self).__init__()
        self.exit = False
        self.start_time = datetime.now()

        # init clients
        self.proxyClient = proxyClient

        # init queues for workers
        self.edrpou_codes_queue = edrpou_codes_queue
        self.edr_ids_queue = edr_ids_queue
        self.upload_to_doc_service_queue = upload_to_doc_service_queue

        # retry queues for workers
        self.retry_edrpou_codes_queue = Queue(maxsize=500)
        self.retry_edr_ids_queue = Queue(maxsize=500)

        # blockers
        self.until_too_many_requests_event = gevent.event.Event()

        self.until_too_many_requests_event.set()

        self.delay = delay
        self.processing_items = processing_items

    def get_edr_id(self):
        """Get data from edrpou_codes_queue; make request to EDR Api, passing EDRPOU (IPN, passport); Received ids is
        put into Data.edr_ids variable; Data variable placed to edr_ids_queue."""
        while not self.exit:
            try:
                tender_data = self.edrpou_codes_queue.peek()
            except LoopExit:
                gevent.sleep(0)
                continue
            logger.info('Get tender {} from edrpou_codes_queue'.format(tender_data.tender_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                              params={"TENDER_ID": tender_data.tender_id}))
            self.until_too_many_requests_event.wait()
            document_id = tender_data.file_content['meta']['id']
            response = self.proxyClient.verify(validate_param(tender_data.code), tender_data.code, headers={'X-Client-Request-ID': document_id})
            if response.status_code == 404 and response.json().get('errors')[0].get('description')[0].get('error').get('code') == u"notFound":
                logger.info('Empty response for tender {} {}.'.format(tender_data.tender_id, document_id),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_EMPTY_RESPONSE},
                                                  params={"TENDER_ID": tender_data.tender_id, "DOCUMENT_ID": document_id}))
                file_content = response.json().get('errors')[0].get('description')[0]
                file_content['meta'].update(tender_data.file_content['meta'])  # add meta.id to file_content
                file_content['meta'].update({"version": version})  # add filed meta.version
                if response.headers.get('X-Request-ID'):
                    file_content['meta']['sourceRequests'].append(response.headers['X-Request-ID'])  # add unique request id
                data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code, tender_data.item_name, [], file_content)
                self.upload_to_doc_service_queue.put(data)  # Given EDRPOU code not found, file with error put into upload_to_doc_service_queue
                self.edrpou_codes_queue.get()
                continue
            if response.status_code == 200:
                # Create new Data object. Write to Data.code list of edr ids from EDR.
                # List because EDR can return 0, 1 or 2 values to our request
                try:
                    tender_data.file_content['meta']['sourceRequests'].append(response.headers['X-Request-ID'])  # add unique request id
                    data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code, tender_data.item_name,
                                [edr_ids['x_edrInternalId'] for edr_ids in response.json().get('data', [])], tender_data.file_content)
                    self.processing_items['{}_{}'.format(tender_data.tender_id, tender_data.item_id)] = len(data.edr_ids)
                except TypeError as e:
                    logger.info('Error data type {} {} {}. {}'.format(tender_data.tender_id, tender_data.item_name, tender_data.item_id, e))
                    self.retry_edrpou_codes_queue.put(tender_data)
                else:
                    self.edr_ids_queue.put(data)
                    logger.info('Put tender {} {} {} to edr_ids_queue.'.format(tender_data.tender_id,
                                                                               tender_data.item_name,
                                                                               tender_data.item_id))
            else:
                if response.headers.get('X-Request-ID'):
                    tender_data.file_content['meta']['sourceRequests'].append(response.headers['X-Request-ID'])
                self.handle_status_response(response, tender_data.tender_id)
                self.retry_edrpou_codes_queue.put(tender_data)  # Put tender to retry
                logger.info('Put tender {} with {} id {} to retry_edrpou_codes_queue'.format(
                    tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                    extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
            self.edrpou_codes_queue.get()
            gevent.sleep(0)

    def retry_get_edr_id(self):
        """Get data from retry_edrpou_codes_queue; Put data into edr_ids_queue if request is successful, otherwise put
        data back to retry_edrpou_codes_queue."""
        while not self.exit:
            try:
                tender_data = self.retry_edrpou_codes_queue.get()
            except LoopExit:
                gevent.sleep(0)
                continue
            logger.info('Get tender {} from retry_edrpou_codes_queue'.format(tender_data.tender_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                              params={"TENDER_ID": tender_data.tender_id}))
            self.until_too_many_requests_event.wait()
            document_id = tender_data.file_content['meta']['id']
            try:
                response = self.get_edr_id_request(validate_param(tender_data.code), tender_data.code, document_id)
                if response.headers.get('X-Request-ID'):
                    tender_data.file_content['meta']['sourceRequests'].append(response.headers['X-Request-ID'])
            except RetryException as re:
                if re.args[1].status_code == 404 and re.args[1].json().get('errors')[0].get('description')[0].get('error').get('code') == u"notFound":
                    logger.info('Empty response for tender {}.{}.'.format(tender_data.tender_id, document_id),
                                extra=journal_context({"MESSAGE_ID": DATABRIDGE_EMPTY_RESPONSE},
                                                      params={"TENDER_ID": tender_data.tender_id, "DOCUMENT_ID": document_id}))
                    file_content = re.args[1].json().get('errors')[0].get('description')[0]
                    file_content['meta'].update(tender_data.file_content['meta'])
                    file_content['meta'].update({"version": version})  # add filed meta.version
                    data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                tender_data.item_name, [], file_content)
                    self.upload_to_doc_service_queue.put(data)  # Given EDRPOU code not found, file with error put into upload_to_doc_service_queue
                    continue
                logger.info("RetryException error message {}".format(re.args[0]))
                self.handle_status_response(re.args[1], tender_data.tender_id)
                self.retry_edrpou_codes_queue.put(tender_data)
                logger.info('Put tender {} with {} id {} to retry_edrpou_codes_queue'.format(
                    tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                    extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
                gevent.sleep(0)
            except Exception:
                self.retry_edrpou_codes_queue.put(tender_data)
                logger.info('Put tender {} with {} id {} to retry_edrpou_codes_queue'.format(
                    tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                    extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
                gevent.sleep(0)
            else:
                # Create new Data object. Write to Data.code list of edr ids from EDR.
                # List because EDR can return 0, 1 or 2 values to our request
                if response.status_code == 429:
                    seconds_to_wait = response.headers.get('Retry-After', self.delay)
                    logger.info('Too many requests to EDR API. Msg: {}, wait {} seconds.'.format(response.text,
                                                                                                 seconds_to_wait))
                    self.wait_until_too_many_requests(seconds_to_wait)
                    self.retry_edrpou_codes_queue.put(tender_data)
                    continue
                if response.status_code == 200:
                    try:
                        data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code, tender_data.item_name,
                                    [obj['x_edrInternalId'] for obj in response.json().get('data', [])], tender_data.file_content)
                        self.processing_items['{}_{}'.format(tender_data.tender_id, tender_data.item_id)] = len(data.edr_ids)
                    except TypeError as e:
                        logger.info('Error data type {} {} {}. {}'.format(tender_data.tender_id, tender_data.item_name,
                                                                          tender_data.item_id, e))
                        self.retry_edrpou_codes_queue.put(tender_data)
                    else:
                        self.edr_ids_queue.put(data)
                        logger.info('Put tender {} {} {} from retry to edr_ids_queue.'.format(tender_data.tender_id,
                                                                                              tender_data.item_name,
                                                                                              tender_data.item_id))
            gevent.sleep(0)

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
    def get_edr_id_request(self, param, code, document_id):
        """Execute request to EDR Api for retry queue objects."""
        self.until_too_many_requests_event.wait()
        response = self.proxyClient.verify(param, code, headers={'X-Client-Request-ID': document_id})
        if response.status_code not in (200, 429):
            logger.info(
                'Get unsuccessful response {} in get_edr_id_request, header {}'.format(response.status_code, response.headers.get('X-Request-ID')))
            raise RetryException('Unsuccessful retry request to EDR.', response)
        return response

    def get_edr_details(self):
        """Get data from edr_ids_queue; make request to EDR Api for detailed info; Required fields is put to
        Data.file_content variable, Data object is put to upload_to_doc_service_queue."""
        while not self.exit:
            try:
                tender_data = self.edr_ids_queue.peek()
            except LoopExit:
                gevent.sleep(0)
                continue
            logger.info('Get edr ids {}  tender {} from edr_ids_queue'.format(tender_data.edr_ids, tender_data.tender_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                              params={"TENDER_ID": tender_data.tender_id}))
            self.until_too_many_requests_event.wait()
            meta_id = tender_data.file_content['meta']['id']
            for edr_id in tender_data.edr_ids:
                # if more then 1 instance add amount and number of document to document_id
                document_id = check_add_suffix(tender_data.edr_ids, meta_id, tender_data.edr_ids.index(edr_id) + 1)
                tender_data.file_content['meta']['id'] = document_id
                response = self.proxyClient.details(id=edr_id, headers={'X-Client-Request-ID': document_id})
                if response.status_code == 200:
                    if not isinstance(response.json(), dict):
                        file_content = tender_data.file_content
                        file_content['meta']['sourceRequests'].append(response.headers['X-Request-ID'])
                        logger.info('Error data type {} {} {} {}. Message {}'.format(
                            tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id, "Not a dictionary"),
                            extra=journal_context({"DOCUMENT_ID": document_id}))
                        self.retry_edr_ids_queue.put(Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                                          tender_data.item_name, [edr_id], file_content))
                    else:
                        file_content = response.json()
                        file_content['meta'].update(deepcopy(tender_data.file_content['meta']))
                        file_content['meta'].update({"version": version})  # add filed meta.version
                        file_content['meta']['sourceRequests'].append(response.headers['X-Request-ID'])
                        data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                    tender_data.item_name, tender_data.edr_ids, file_content)
                        self.upload_to_doc_service_queue.put(data)
                        logger.info('Successfully created file for tender {} {} {} {}.'.format(
                            tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_CREATE_FILE},
                                                    params={"TENDER_ID": tender_data.tender_id, "DOCUMENT_ID": document_id}))
                else:

                    file_content = tender_data.file_content
                    if response.headers.get('X-Request-ID'):
                        file_content['meta']['sourceRequests'].append(response.headers['X-Request-ID'])
                    self.handle_status_response(response, tender_data.tender_id)
                    self.retry_edr_ids_queue.put(Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                                      tender_data.item_name, [edr_id], file_content))
                    logger.info('Put tender {} with {} id {} document_id {}  to retry_edr_ids_queue'.format(
                                tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id),
                                extra=journal_context(params={"TENDER_ID": tender_data.tender_id, "DOCUMENT_ID": document_id}))
            self.edr_ids_queue.get()
            gevent.sleep(0)

    def retry_get_edr_details(self):
        """Get data from retry_edr_ids_queue; Put data into upload_to_doc_service_queue if request is successful, otherwise put
        data back to retry_edr_ids_queue."""
        while not self.exit:
            try:
                tender_data = self.retry_edr_ids_queue.get()
            except LoopExit:
                gevent.sleep(0)
                continue
            logger.info('Get edr ids {}  tender {} from retry_edr_ids_queue'.format(tender_data.edr_ids,
                                                                                    tender_data.tender_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                              params={"TENDER_ID": tender_data.tender_id}))
            self.until_too_many_requests_event.wait()
            document_id = tender_data.file_content['meta']['id']
            for edr_id in tender_data.edr_ids:
                try:
                    response = self.get_edr_details_request(edr_id, document_id)
                    if response.headers.get('X-Request-ID'):
                        tender_data.file_content['meta']['sourceRequests'].append(response.headers['X-Request-ID'])
                except RetryException as re:
                    self.handle_status_response(re.args[1], tender_data.tender_id)
                    self.retry_edr_ids_queue.put((Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                                       tender_data.item_name, [edr_id], tender_data.file_content)))
                    logger.info('Put tender {} with {} id {} {} to retry_edr_ids_queue. Error response {}'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id, re.args[1].json().get('errors')),
                        extra=journal_context(params={"TENDER_ID": tender_data.tender_id, "DOCUMENT_ID": document_id}))
                    gevent.sleep(0)
                else:
                    if response.status_code == 429:
                        seconds_to_wait = response.headers.get('Retry-After', self.delay)
                        logger.info('Too many requests to EDR API. Msg: {}, wait {} seconds.'.format(response.text,
                                                                                                     seconds_to_wait))
                        self.retry_edr_ids_queue.put(tender_data)
                        self.wait_until_too_many_requests(seconds_to_wait)
                        continue
                    if not isinstance(response.json(), dict):
                        logger.info('Error data type {} {} {} {}. Message {}.'.format(
                            tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id, "Not a dictionary"))
                        self.retry_edr_ids_queue.put((Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                                           tender_data.item_name, [edr_id], tender_data.file_content)))
                    else:
                        file_content = response.json()
                        file_content['meta'].update(tender_data.file_content['meta'])
                        file_content['meta'].update({"version": version})  # add filed meta.version
                        data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                    tender_data.item_name, tender_data.edr_ids, file_content)
                        self.upload_to_doc_service_queue.put(data)
                        logger.info('Successfully created file for tender {} {} {} {} in retry.'.format(
                            tender_data.tender_id, tender_data.item_name, tender_data.item_id, document_id),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_CREATE_FILE},
                                                  params={"TENDER_ID": tender_data.tender_id,  "DOCUMENT_ID": document_id}))
            gevent.sleep(0)

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
    def get_edr_details_request(self, edr_id, document_id):
        """Execute request to EDR Api to get detailed info for retry queue objects."""
        self.until_too_many_requests_event.wait()
        response = self.proxyClient.details(id=edr_id, headers={'X-Client-Request-ID': document_id})
        if response.status_code not in (200, 429):
            logger.info(
                'Get unsuccessful response {} in get_edr_details_request, header {}'.format(response.status_code, response.headers.get('X-Request-ID')))
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
        self.immortal_jobs = {'get_edr_id': spawn(self.get_edr_id),
                              'get_edr_details': spawn(self.get_edr_details),
                              'retry_get_edr_id': spawn(self.retry_get_edr_id),
                              'retry_get_edr_details': spawn(self.retry_get_edr_details)}

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
