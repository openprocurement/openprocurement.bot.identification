# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()

import logging.config
import gevent

from retrying import retry
from gevent.queue import Queue
from datetime import datetime
from gevent.hub import LoopExit
from gevent import spawn

from openprocurement.bot.identification.databridge.base_worker import BaseWorker
from openprocurement.bot.identification.databridge.utils import journal_context, create_file
from openprocurement.bot.identification.databridge.journal_msg_ids import DATABRIDGE_SUCCESS_UPLOAD_TO_DOC_SERVICE, \
    DATABRIDGE_UNSUCCESS_UPLOAD_TO_DOC_SERVICE
from openprocurement.bot.identification.databridge.constants import file_name, retry_mult

logger = logging.getLogger(__name__)


class UploadFileToDocService(BaseWorker):
    """ Upload file with details """

    def __init__(self, upload_to_doc_service_queue, upload_to_tender_queue, process_tracker, doc_service_client,
                 services_not_available, sleep_change_value, delay=15):
        super(UploadFileToDocService, self).__init__(services_not_available)
        self.start_time = datetime.now()

        self.delay = delay
        self.process_tracker = process_tracker
        # init client
        self.doc_service_client = doc_service_client

        # init queues for workers
        self.upload_to_doc_service_queue = upload_to_doc_service_queue
        self.upload_to_tender_queue = upload_to_tender_queue
        self.sleep_change_value = sleep_change_value
        # retry queues for workers
        self.retry_upload_to_doc_service_queue = Queue(maxsize=500)

    def upload_worker(self):
        while not self.exit:
            self.services_not_available.wait()
            self.try_peek_and_upload(False)
            gevent.sleep(0)

    def retry_upload_worker(self):
        while not self.exit:
            self.services_not_available.wait()
            self.try_peek_and_upload(True)
            gevent.sleep(0)

    def try_peek_and_upload(self, is_retry):
        try:
            tender_data = self.peek_from_queue(is_retry)
        except LoopExit:
            gevent.sleep(0)
        else:
            self.try_upload_to_doc_service(tender_data, is_retry)

    def peek_from_queue(self, is_retry):
        return self.retry_upload_to_doc_service_queue.peek() if is_retry else self.upload_to_doc_service_queue.peek()

    def try_upload_to_doc_service(self, tender_data, is_retry):
        try:
            response = self.update_headers_and_upload(tender_data, is_retry)
        except Exception as e:
            self.remove_bad_data(tender_data, e, is_retry)
        else:
            self.move_to_tender_if_200(response, tender_data, is_retry)

    def update_headers_and_upload(self, tender_data, is_retry):
        if is_retry:
            return self.update_headers_and_upload_retry(tender_data)
        else:
            return self.doc_service_client.upload(file_name, create_file(tender_data.file_content), 'application/yaml',
                                                  headers={'X-Client-Request-ID': tender_data.doc_id()})

    def update_headers_and_upload_retry(self, tender_data):
        self.doc_service_client.headers.update({'X-Client-Request-ID': tender_data.doc_id()})
        return self.client_upload_to_doc_service(tender_data)

    def remove_bad_data(self, tender_data, e, is_retry):
        logger.exception('Exception while uploading file to doc service {} doc_id: {}. Message: {}. {}'.
                         format(tender_data, tender_data.doc_id(), e, "Removed tender data" if is_retry else ""),
                         extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_UPLOAD_TO_DOC_SERVICE},
                                               tender_data.log_params()))
        if is_retry:
            self.retry_upload_to_doc_service_queue.get()
            self.process_tracker.update_items_and_tender(tender_data.tender_id, tender_data.item_id,
                                                         tender_data.doc_id())
            raise e
        else:
            self.retry_upload_to_doc_service_queue.put(tender_data)
            self.upload_to_doc_service_queue.get()

    def move_to_tender_if_200(self, response, tender_data, is_retry):
        if response.status_code == 200:
            self.move_to_tender_queue(tender_data, response, is_retry)
        else:
            self.move_data_to_retry_or_leave(response, tender_data, is_retry)

    def move_to_tender_queue(self, tender_data, response, is_retry):
        data = tender_data
        data.file_content = dict(response.json(), **{'meta': {'id': tender_data.doc_id()}})
        self.upload_to_tender_queue.put(data)
        if not is_retry:
            self.upload_to_doc_service_queue.get()
        else:
            self.retry_upload_to_doc_service_queue.get()
        logger.info('Successfully uploaded file to doc service {} doc_id: {}'.format(tender_data, tender_data.doc_id()),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_UPLOAD_TO_DOC_SERVICE},
                                          tender_data.log_params()))

    def move_data_to_retry_or_leave(self, response, tender_data, is_retry):
        logger.info('Not successful response from document service while uploading {} doc_id: {}. Response {}'.
                    format(tender_data, tender_data.doc_id(), response.status_code),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_UPLOAD_TO_DOC_SERVICE},
                                          tender_data.log_params()))
        if not is_retry:
            self.retry_upload_to_doc_service_queue.put(tender_data)
            self.upload_to_doc_service_queue.get()

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=retry_mult)
    def client_upload_to_doc_service(self, tender_data):
        """Process upload request for retry queue objects."""
        return self.doc_service_client.upload(file_name, create_file(tender_data.file_content), 'application/yaml',
                                              headers={'X-Client-Request-ID': tender_data.doc_id()})

    def _start_jobs(self):
        return {'upload_worker': spawn(self.upload_worker),
                'retry_upload_worker': spawn(self.retry_upload_worker)}
