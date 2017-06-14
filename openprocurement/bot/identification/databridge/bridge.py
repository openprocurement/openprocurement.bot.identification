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
from gevent.queue import Queue
from restkit import request, RequestError
from requests import RequestException

from openprocurement_client.client import TendersClientSync as BaseTendersClientSync, TendersClient as BaseTendersClient
from openprocurement.bot.identification.client import DocServiceClient, ProxyClient
from openprocurement.bot.identification.databridge.scanner import Scanner
from openprocurement.bot.identification.databridge.filter_tender import FilterTenders
from openprocurement.bot.identification.databridge.edr_handler import EdrHandler
from openprocurement.bot.identification.databridge.upload_file import UploadFile
from openprocurement.bot.identification.databridge.utils import journal_context, check_412
from openprocurement.bot.identification.databridge.journal_msg_ids import (
    DATABRIDGE_RESTART_WORKER, DATABRIDGE_START, DATABRIDGE_DOC_SERVICE_CONN_ERROR,
    DATABRIDGE_PROXY_SERVER_CONN_ERROR)

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
        api_version = self.config_get('tenders_api_version')
        ro_api_server = self.config_get('public_tenders_api_server') or api_server
        buffers_size = self.config_get('buffers_size') or 500
        self.delay = self.config_get('delay') or 15
        self.increment_step = self.config_get('increment_step') or 1
        self.decrement_step = self.config_get('decrement_step') or 1
        self.doc_service_host = self.config_get('doc_service_server')
        self.doc_service_port = self.config_get('doc_service_port') or 6555

        # init clients
        self.tenders_sync_client = TendersClientSync('', host_url=ro_api_server, api_version=api_version)
        self.client = TendersClient(self.config_get('api_token'), host_url=api_server, api_version=api_version)
        self.proxyClient = ProxyClient(host=self.config_get('proxy_server'),
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
        # edr_ids_queue - queue with unique identification of the edr object (Data.edr_ids in Data object),
        # received from EDR Api. Later used to make second request to EDR to get detailed info
        self.edr_ids_queue = Queue(maxsize=buffers_size)
        self.upload_to_doc_service_queue = Queue(maxsize=buffers_size)  # queue with detailed info from EDR (Data.file_content)
        # upload_to_tender_queue - queue with  file's get_url
        self.upload_to_tender_queue = Queue(maxsize=buffers_size)

        # blockers
        self.initialization_event = gevent.event.Event()

        # dictionary with processing awards/qualifications
        self.processing_items = {}

        # Workers
        self.scanner = partial(Scanner.spawn,
                               tenders_sync_client=self.tenders_sync_client,
                               filtered_tender_ids_queue=self.filtered_tender_ids_queue,
                               increment_step=self.increment_step,
                               decrement_step=self.decrement_step,
                               delay=self.delay)

        self.filter_tender = partial(FilterTenders.spawn,
                                     tenders_sync_client=self.tenders_sync_client,
                                     filtered_tender_ids_queue=self.filtered_tender_ids_queue,
                                     edrpou_codes_queue=self.edrpou_codes_queue,
                                     processing_items=self.processing_items,
                                     increment_step=self.increment_step,
                                     decrement_step=self.decrement_step,
                                     delay=self.delay)

        self.edr_handler = partial(EdrHandler.spawn,
                                   proxyClient=self.proxyClient,
                                   edrpou_codes_queue=self.edrpou_codes_queue,
                                   edr_ids_queue=self.edr_ids_queue,
                                   upload_to_doc_service_queue=self.upload_to_doc_service_queue,
                                   processing_items=self.processing_items,
                                   delay=self.delay)

        self.upload_file = partial(UploadFile.spawn,
                                   client=self.client,
                                   upload_to_doc_service_queue=self.upload_to_doc_service_queue,
                                   upload_to_tender_queue=self.upload_to_tender_queue,
                                   processing_items=self.processing_items,
                                   doc_service_client=self.doc_service_client,
                                   delay=self.delay)

    def config_get(self, name):
        return self.config.get('main').get(name)

    def check_doc_service(self):
        try:
            request("{host}:{port}/".format(host=self.doc_service_host, port=self.doc_service_port))
        except RequestError as e:
            logger.info('DocService connection error, message {}'.format(e),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_DOC_SERVICE_CONN_ERROR}, {}))
            raise e
        else:
            return True

    def check_proxy(self):
        try:
            self.proxyClient.health()
        except RequestException as e:
            logger.info('Proxy server connection error, message {}'.format(e),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_PROXY_SERVER_CONN_ERROR}, {}))
            raise e
        else:
            return True

    def _start_jobs(self):
        self.jobs = {'scanner': self.scanner(),
                     'filter_tender': self.filter_tender(),
                     'edr_handler': self.edr_handler(),
                     'upload_file': self.upload_file()}

    def run(self):
        logger.info('Start EDR API Data Bridge', extra=journal_context({"MESSAGE_ID": DATABRIDGE_START}, {}))
        self._start_jobs()
        counter = 0
        try:
            while True:
                gevent.sleep(self.delay)
                if counter == 20:
                    logger.info('Current state: Filtered tenders {}; Edrpou codes queue {}; Retry edrpou codes queue {}; '
                                'Edr ids queue {}; Retry edr ids queue {}; Upload to doc service {}; Retry upload to doc service {}; '
                                'Upload to tender {}; Retry upload to tender {}'.format(
                                    self.filtered_tender_ids_queue.qsize(),
                                    self.edrpou_codes_queue.qsize(),
                                    self.jobs['edr_handler'].retry_edrpou_codes_queue.qsize() if self.jobs['edr_handler'] else 0,
                                    self.edr_ids_queue.qsize(),
                                    self.jobs['edr_handler'].retry_edr_ids_queue.qsize() if self.jobs['edr_handler'] else 0,
                                    self.upload_to_doc_service_queue.qsize(),
                                    self.jobs['upload_file'].retry_upload_to_doc_service_queue.qsize() if self.jobs['upload_file'] else 0,
                                    self.upload_to_tender_queue.qsize(),
                                    self.jobs['upload_file'].retry_upload_to_tender_queue.qsize() if self.jobs['upload_file'] else 0
                                ))
                    counter = 0
                counter += 1
                for name, job in self.jobs.items():
                    if job.dead:
                        logger.warning('Restarting {} worker'.format(name),
                                       extra=journal_context({"MESSAGE_ID": DATABRIDGE_RESTART_WORKER}))
                        self.jobs[name] = gevent.spawn(getattr(self, name))
        except KeyboardInterrupt:
            logger.info('Exiting...')
            gevent.killall(self.jobs, timeout=5)
        except Exception as e:
            logger.error(e)


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
        bridge.check_proxy() and bridge.check_doc_service()
        bridge.run()
    else:
        logger.info('Invalid configuration file. Exiting...')


if __name__ == "__main__":
    main()
