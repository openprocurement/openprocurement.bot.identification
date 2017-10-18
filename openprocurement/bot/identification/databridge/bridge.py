# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()

import logging
import logging.config
import os
import argparse
import gevent

from functools import partial
from yaml import load
from pickle import loads
from gevent import event
from gevent.queue import Queue
from retrying import retry
from restkit import request, RequestError, ResourceError
from requests import RequestException
from constants import retry_mult

from openprocurement_client.client import TendersClientSync as BaseTendersClientSync, TendersClient as BaseTendersClient
from openprocurement.bot.identification.client import DocServiceClient, ProxyClient
from openprocurement.bot.identification.databridge.scanner import Scanner
from openprocurement.bot.identification.databridge.filter_tender import FilterTenders
from openprocurement.bot.identification.databridge.edr_handler import EdrHandler
from openprocurement.bot.identification.databridge.upload_file_to_tender import UploadFileToTender
from openprocurement.bot.identification.databridge.upload_file_to_doc_service import UploadFileToDocService
from openprocurement.bot.identification.databridge.utils import journal_context, check_412
from openprocurement.bot.identification.databridge.process_tracker import ProcessTracker
from caching import Db
from openprocurement.bot.identification.databridge.journal_msg_ids import (
    DATABRIDGE_RESTART_WORKER, DATABRIDGE_START, DATABRIDGE_DOC_SERVICE_CONN_ERROR,
    DATABRIDGE_PROXY_SERVER_CONN_ERROR)

from openprocurement.bot.identification.databridge.sleep_change_value import APIRateController

logger = logging.getLogger(__name__)


class TendersClientSync(BaseTendersClientSync):
    @check_412
    def request(self, *args, **kwargs):
        return super(TendersClientSync, self).request(*args, **kwargs)


class TendersClient(BaseTendersClient):
    @check_412
    def _create_tender_resource_item(self, *args, **kwargs):
        return super(TendersClient, self)._create_tender_resource_item(*args, **kwargs)


class EdrDataBridge(object):
    """ Edr API Data Bridge """

    def __init__(self, config):
        super(EdrDataBridge, self).__init__()
        self.config = config

        api_server = self.config_get('tenders_api_server')
        self.api_version = self.config_get('tenders_api_version')
        ro_api_server = self.config_get('public_tenders_api_server') or api_server
        buffers_size = self.config_get('buffers_size') or 500
        self.delay = self.config_get('delay') or 15
        self.increment_step = self.config_get('increment_step') or 1
        self.decrement_step = self.config_get('decrement_step') or 1
        self.sleep_change_value = APIRateController(self.increment_step, self.decrement_step)
        self.doc_service_host = self.config_get('doc_service_server')
        self.doc_service_port = self.config_get('doc_service_port') or 6555
        self.sandbox_mode = os.environ.get('SANDBOX_MODE', 'False')
        self.time_to_live = self.config_get('time_to_live') or 300

        # init clients
        self.tenders_sync_client = TendersClientSync('', host_url=ro_api_server, api_version=self.api_version)
        self.client = TendersClient(self.config_get('api_token'), host_url=api_server, api_version=self.api_version)
        self.proxy_client = ProxyClient(host=self.config_get('proxy_server'),
                                        user=self.config_get('proxy_user'),
                                        password=self.config_get('proxy_password'),
                                        port=self.config_get('proxy_port'),
                                        version=self.config_get('proxy_version'))
        self.doc_service_client = DocServiceClient(host=self.doc_service_host,
                                                   port=self.doc_service_port,
                                                   user=self.config_get('doc_service_user'),
                                                   password=self.config_get('doc_service_password'))

        # init queues for workers
        self.filtered_tender_ids_queue = Queue(maxsize=buffers_size)  # queue of tender IDs with appropriate status
        self.edrpou_codes_queue = Queue(maxsize=buffers_size)  # queue with edrpou codes (Data objects stored in it)
        self.upload_to_doc_service_queue = Queue(maxsize=buffers_size)  # queue with info from EDR (Data.file_content)
        self.upload_to_tender_queue = Queue(maxsize=buffers_size)

        # blockers
        self.initialization_event = event.Event()
        self.services_not_available = event.Event()
        self.services_not_available.set()
        self.db = Db(config)
        self.process_tracker = ProcessTracker(self.db, self.time_to_live)
        unprocessed_items = self.process_tracker.get_unprocessed_items()
        for item in unprocessed_items:
            self.upload_to_doc_service_queue.put(loads(item))

        # Workers
        self.scanner = partial(Scanner.spawn,
                               tenders_sync_client=self.tenders_sync_client,
                               filtered_tender_ids_queue=self.filtered_tender_ids_queue,
                               services_not_available=self.services_not_available,
                               process_tracker=self.process_tracker,
                               sleep_change_value=self.sleep_change_value,
                               delay=self.delay)

        self.filter_tender = partial(FilterTenders.spawn,
                                     tenders_sync_client=self.tenders_sync_client,
                                     filtered_tender_ids_queue=self.filtered_tender_ids_queue,
                                     edrpou_codes_queue=self.edrpou_codes_queue,
                                     process_tracker=self.process_tracker,
                                     services_not_available=self.services_not_available,
                                     sleep_change_value=self.sleep_change_value,
                                     delay=self.delay)

        self.edr_handler = partial(EdrHandler.spawn,
                                   proxy_client=self.proxy_client,
                                   edrpou_codes_queue=self.edrpou_codes_queue,
                                   upload_to_doc_service_queue=self.upload_to_doc_service_queue,
                                   process_tracker=self.process_tracker,
                                   services_not_available=self.services_not_available,
                                   delay=self.delay)

        self.upload_file_to_doc_service = partial(UploadFileToDocService.spawn,
                                                  upload_to_doc_service_queue=self.upload_to_doc_service_queue,
                                                  upload_to_tender_queue=self.upload_to_tender_queue,
                                                  process_tracker=self.process_tracker,
                                                  doc_service_client=self.doc_service_client,
                                                  services_not_available=self.services_not_available,
                                                  sleep_change_value=self.sleep_change_value,
                                                  delay=self.delay)

        self.upload_file_to_tender = partial(UploadFileToTender.spawn,
                                             client=self.client,
                                             upload_to_tender_queue=self.upload_to_tender_queue,
                                             process_tracker=self.process_tracker,
                                             services_not_available=self.services_not_available,
                                             sleep_change_value=self.sleep_change_value,
                                             delay=self.delay)

    def config_get(self, name):
        return self.config.get('main').get(name)

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=retry_mult)
    def check_doc_service(self):
        try:
            request("{host}:{port}/".format(host=self.doc_service_host, port=self.doc_service_port))
        except RequestError as e:
            logger.info('DocService connection error, message {}'.format(e),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_DOC_SERVICE_CONN_ERROR}, {}))
            raise e
        else:
            return True

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=retry_mult)
    def check_proxy(self):
        """Check whether proxy is up and has the same sandbox mode (to prevent launching wrong pair of bot-proxy)"""
        try:
            self.proxy_client.health(self.sandbox_mode)
        except RequestException as e:
            logger.info('Proxy server connection error, message {} {}'.format(e, self.sandbox_mode),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_PROXY_SERVER_CONN_ERROR}, {}))
            raise e
        else:
            return True

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=retry_mult)
    def check_openprocurement_api(self):
        """Makes request to the TendersClient, returns True if it's up, raises RequestError otherwise"""
        try:
            self.client.head('/api/{}/spore'.format(self.api_version))
        except (RequestError, ResourceError) as e:
            logger.info('TendersServer connection error, message {}'.format(e),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_DOC_SERVICE_CONN_ERROR}, {}))
            raise e
        else:
            return True

    def set_sleep(self):
        self.services_not_available.clear()

    def set_wake_up(self):
        self.services_not_available.set()

    def all_available(self):
        try:
            self.check_proxy() and self.check_openprocurement_api() and self.check_doc_service()
        except Exception as e:
            logger.info("Service is unavailable, message {}".format(e.message))
            return False
        else:
            return True

    def check_services(self):
        if self.all_available():
            logger.info("All services are available")
            self.set_wake_up()
        else:
            logger.info("Pausing bot")
            self.set_sleep()

    def _start_jobs(self):
        self.jobs = {'scanner': self.scanner(),
                     'filter_tender': self.filter_tender(),
                     'edr_handler': self.edr_handler(),
                     'upload_file_to_doc_service': self.upload_file_to_doc_service(),
                     'upload_file_to_tender': self.upload_file_to_tender(),
                     }

    def launch(self):
        while True:
            if self.all_available():
                self.run()
                break
            gevent.sleep(self.delay)

    def run(self):
        logger.info('Start EDR API Data Bridge', extra=journal_context({"MESSAGE_ID": DATABRIDGE_START}, {}))
        self._start_jobs()
        counter = 0
        try:
            while True:
                gevent.sleep(self.delay)
                self.check_services()
                if counter == 20:
                    counter = 0
                    logger.info(
                        'Current state: Filtered tenders {}; Edrpou codes queue {}; Retry edrpou codes queue {};'
                        'Upload to doc service {}; Retry upload to doc service {}; '
                        'Upload to tender {}; Retry upload to tender {}'.format(
                            self.filtered_tender_ids_queue.qsize(),
                            self.edrpou_codes_queue.qsize(),
                            self.jobs['edr_handler'].retry_edrpou_codes_queue.qsize() if self.jobs[
                                'edr_handler'] else 0,
                            self.upload_to_doc_service_queue.qsize(),
                            self.jobs['upload_file_to_doc_service'].retry_upload_to_doc_service_queue.qsize() if
                            self.jobs[
                                'upload_file_to_doc_service'] else 0,
                            self.upload_to_tender_queue.qsize(),
                            self.jobs['upload_file_to_tender'].retry_upload_to_tender_queue.qsize() if self.jobs[
                                'upload_file_to_tender'] else 0
                        ))
                counter += 1
                self.check_and_revive_jobs()
        except KeyboardInterrupt:
            logger.info('Exiting...')
            gevent.killall(self.jobs, timeout=5)
        except Exception as e:
            logger.error(e)

    def check_and_revive_jobs(self):
        for name, job in self.jobs.items():
            if job.dead:
                self.revive_job(name)

    def revive_job(self, name):
        logger.warning('Restarting {} worker'.format(name),
                       extra=journal_context({"MESSAGE_ID": DATABRIDGE_RESTART_WORKER}))
        self.jobs[name] = gevent.spawn(getattr(self, name))


def main():
    parser = argparse.ArgumentParser(description='Edr API Data Bridge')
    parser.add_argument('config', type=str, help='Path to configuration file')
    parser.add_argument('--tender', type=str, help='Tender id to sync', dest="tender_id")
    params = parser.parse_args()
    if os.path.isfile(params.config):
        with open(params.config) as config_file_obj:
            config = load(config_file_obj.read())
        logging.config.dictConfig(config)
        bridge = EdrDataBridge(config)
        bridge.launch()
    else:
        logger.info('Invalid configuration file. Exiting...')


if __name__ == "__main__":
    main()
