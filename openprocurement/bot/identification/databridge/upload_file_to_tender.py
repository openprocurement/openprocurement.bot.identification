# coding=utf-8
from munch import munchify
from gevent.queue import Queue
from retrying import retry

import logging.config
import gevent
from datetime import datetime
from gevent import Greenlet, spawn
from gevent.hub import LoopExit
from restkit import ResourceError

from openprocurement.bot.identification.databridge.utils import journal_context
from openprocurement.bot.identification.databridge.journal_msg_ids import DATABRIDGE_SUCCESS_UPLOAD_TO_TENDER, \
    DATABRIDGE_UNSUCCESS_UPLOAD_TO_TENDER, DATABRIDGE_ITEM_STATUS_CHANGED_WHILE_PROCESSING, DATABRIDGE_START_UPLOAD
from openprocurement.bot.identification.databridge.constants import retry_mult


logger = logging.getLogger(__name__)


class UploadFileToTender(Greenlet):

    def __init__(self, client, upload_to_tender_queue, process_tracker, services_not_available, sleep_change_value, delay=15):
        super(UploadFileToTender, self).__init__()
        self.exit = False
        self.start_time = datetime.now()

        self.delay = delay
        self.process_tracker = process_tracker

        # init clients
        self.client = client

        # init queues for workers
        self.upload_to_tender_queue = upload_to_tender_queue
        self.retry_upload_to_tender_queue = Queue(maxsize=500)

        # blockers
        self.services_not_available = services_not_available
        self.sleep_change_value = sleep_change_value

    def upload_to_tender(self):
        """Get data from upload_to_tender_queue; Upload get_Url and documentType;
        If upload to tender were unsuccessful put Data object to retry_upload_to_tender_queue, otherwise delete given
        award/qualification from processing_items."""
        while not self.exit:
            self.services_not_available.wait()
            self.try_peek_data_and_upload_to_tender(False)
            gevent.sleep(self.sleep_change_value.time_between_requests)

    def retry_upload_to_tender(self):
        """Get data from retry_upload_to_tender_queue; If upload was unsuccessful put Data obj back to
        retry_upload_to_tender_queue"""
        while not self.exit:
            self.services_not_available.wait()
            self.try_peek_data_and_upload_to_tender(True)
            gevent.sleep(self.sleep_change_value.time_between_requests)

    def try_peek_data_and_upload_to_tender(self, is_retry):
        try:
            tender_data = self.peek_from_tender_queue(is_retry)
        except LoopExit:
            gevent.sleep(0)
        else:
            self.try_upload_to_tender(tender_data, is_retry)

    def peek_from_tender_queue(self, is_retry):
        return self.retry_upload_to_tender_queue.peek() if is_retry else self.upload_to_tender_queue.peek()

    def try_upload_to_tender(self, tender_data, is_retry):
        try:
            self.update_headers_and_upload_to_tender(tender_data, is_retry)
        except ResourceError as re:
            self.remove_data_or_increase_wait(re, tender_data, is_retry)
        except Exception as e:
            self.handle_error(e, tender_data, is_retry)
        else:
            self.succesfully_uploaded_to_tender(tender_data, is_retry)

    def update_headers_and_upload_to_tender(self, tender_data, is_retry):
        if is_retry:
            self.do_upload_to_tender_with_retry(tender_data)
        else:
            self.do_upload_to_tender(tender_data)

    def do_upload_to_tender(self, tender_data):
        document_data = tender_data.file_content.get('data', {})
        document_data["documentType"] = "registerExtract"
        self.client.headers.update({'X-Client-Request-ID': tender_data.doc_id()})
        self.client._create_tender_resource_item(munchify({'data': {'id': tender_data.tender_id}}),
                                                 {'data': document_data},
                                                 '{}/{}/documents'.format(tender_data.item_name, tender_data.item_id))

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=retry_mult)
    def do_upload_to_tender_with_retry(self, tender_data):
        """Process upload to tender request for retry queue objects."""
        self.do_upload_to_tender(tender_data)

    def remove_data_or_increase_wait(self, re, tender_data, is_retry):
        if re.status_int == 403 or re.status_int == 422 or re.status_int is None:
            self.removing_data(re, tender_data, is_retry)
        elif re.status_int == 429:
            self.decrease_request_frequency(re, tender_data)
        else:
            self.handle_error(re, tender_data, is_retry)

    def removing_data(self, re, tender_data, is_retry):
        logger.warning("Accept {} while uploading to {} doc_id: {}. Message {}".format(
            re.status_int, tender_data, tender_data.doc_id(), re.msg),
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_ITEM_STATUS_CHANGED_WHILE_PROCESSING},
                                  tender_data.log_params()))
        self.process_tracker.update_items_and_tender(tender_data.tender_id, tender_data.item_id)
        self.sleep_change_value.decrement()
        if is_retry:
            self.retry_upload_to_tender_queue.get()
        else:
            self.upload_to_tender_queue.get()

    def decrease_request_frequency(self, re, tender_data):
        logger.info("Accept 429 while uploading to {} doc_id: {}. Message {}".format(
            tender_data, tender_data.doc_id(), re.msg),
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_ITEM_STATUS_CHANGED_WHILE_PROCESSING},
                                  tender_data.log_params()))
        self.sleep_change_value.increment()

    def handle_error(self, re, tender_data, is_retry):
        logger.info('Error while uploading file to {} doc_id: {}. Status: {}. Message: {}'.format(
            tender_data, tender_data.doc_id(), getattr(re, "status_int", None), re.message),
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_UPLOAD_TO_TENDER}, tender_data.log_params()))
        self.sleep_change_value.decrement()
        if not is_retry:
            self.retry_upload_to_tender_queue.put(tender_data)
            self.upload_to_tender_queue.get()

    def succesfully_uploaded_to_tender(self, tender_data, is_retry):
        logger.info('Successfully uploaded file to {} doc_id: {}'.format(tender_data, tender_data.doc_id()),
                extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_UPLOAD_TO_TENDER}, tender_data.log_params()))
        # delete current tender after successful upload file (to avoid reloading file)
        self.process_tracker.update_items_and_tender(tender_data.tender_id, tender_data.item_id)
        self.sleep_change_value.decrement()
        if is_retry:
            self.retry_upload_to_tender_queue.get()
        else:
            self.upload_to_tender_queue.get()

    def _start_jobs(self):
        return {'upload_to_tender': spawn(self.upload_to_tender),
                'retry_upload_to_tender': spawn(self.retry_upload_to_tender)}

    def _run(self):
        logger.info('Start UploadFileToTender worker',
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_START_UPLOAD}, {}))
        self.immortal_jobs = self._start_jobs()
        try:
            while not self.exit:
                gevent.sleep(self.delay)
                self.check_and_revive_jobs()
        except Exception as e:
            logger.error(e)
            gevent.killall(self.immortal_jobs.values(), timeout=5)

    def check_and_revive_jobs(self):
        for name, job in self.immortal_jobs.items():
            if job.dead:
                logger.warning("{} worker dead try restart".format(name), extra=journal_context(
                    {"MESSAGE_ID": 'DATABRIDGE_RESTART_{}'.format(name.lower())}, {}))
                self.immortal_jobs[name] = gevent.spawn(getattr(self, name))
                logger.info("{} worker is up".format(name))

    def shutdown(self):
        self.exit = True
        logger.info('Worker UploadFileToTender complete his job.')

