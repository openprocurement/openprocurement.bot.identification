# -*- coding: utf-8 -*-

import unittest
import os
import logging
import time

from requests import RequestException

from mock import patch, MagicMock
from restkit import RequestError

from gevent.pywsgi import WSGIServer
from bottle import Bottle, response

from openprocurement.bot.identification.databridge.bridge import EdrDataBridge
from openprocurement_client.client import TendersClientSync, TendersClient
from openprocurement.bot.identification.client import DocServiceClient, ProxyClient
from openprocurement.bot.identification.databridge.constants import test_x_edr_internal_id
from openprocurement.bot.identification.tests.utils import MockLoggingHandler




config = {
    'main':
        {
            'tenders_api_server': 'http://127.0.0.1:20604',
            'tenders_api_version': '2.3',
            'public_tenders_api_server': 'http://127.0.0.1:20605',
            'doc_service_server': 'http://127.0.0.1',
            'doc_service_port': 20606,
            'doc_service_user': 'broker',
            'doc_service_password': 'broker_pass',
            'proxy_server': 'http://127.0.0.1',
            'proxy_port': 20607,
            'proxy_user': 'robot',
            'proxy_password': 'robot',
            'proxy_version': 1.0,
            'buffers_size': 450,
            'full_stack_sync_delay': 15,
            'empty_stack_sync_delay': 101,
            'on_error_sleep_delay': 5,
            'api_token': "api_token",
            # 'delay': 0.1
        }
}


class AlmostAlwaysTrue(object):

    def __init__(self, total_iterations=1):
        self.total_iterations = total_iterations
        self.current_iteration = 0

    def __nonzero__(self):
        if self.current_iteration < self.total_iterations:
            self.current_iteration += 1
            return bool(1)
        return bool(0)


class BaseServersTest(unittest.TestCase):
    """Api server to test openprocurement.integrations.edr.databridge.bridge """

    relative_to = os.path.dirname(__file__)  # crafty line

    @classmethod
    def setUpClass(cls):
        cls.api_server_bottle = Bottle()
        cls.proxy_server_bottle = Bottle()
        cls.doc_server_bottle = Bottle()
        cls.api_server = WSGIServer(('127.0.0.1', 20604), cls.api_server_bottle, log=None)
        setup_routing(cls.api_server_bottle, response_spore, method='HEAD')
        cls.public_api_server = WSGIServer(('127.0.0.1', 20605), cls.api_server_bottle, log=None)
        cls.doc_server = WSGIServer(('127.0.0.1', 20606), cls.doc_server_bottle, log=None)
        setup_routing(cls.doc_server_bottle, doc_response, path='/')
        cls.proxy_server = WSGIServer(('127.0.0.1', 20607), cls.proxy_server_bottle, log=None)
        setup_routing(cls.proxy_server_bottle, proxy_response, path='/api/1.0/health')

        # start servers
        cls.api_server.start()
        cls.public_api_server.start()
        cls.doc_server.start()
        cls.proxy_server.start()

        # mock logging
        logger = logging.getLogger("openprocurement.bot.identification.databridge.bridge")
        cls.log_handler = MockLoggingHandler(level='DEBUG')
        logger.addHandler(cls.log_handler)
        cls.log_messages = cls.log_handler.messages

    @classmethod
    def tearDownClass(cls):
        cls.api_server.close()
        cls.public_api_server.close()
        cls.doc_server.close()
        cls.proxy_server.close()

    def setUp(self):
        super(BaseServersTest, self).setUp()
        self.log_handler.reset()

    def tearDown(self):
        setup_routing(self.proxy_server_bottle, proxy_response, path='/api/1.0/health')
        del self.worker


def setup_routing(app, func, path='/api/2.3/spore', method='GET'):
    app.routes = []
    app.route(path, method, func)

def setup_routing_multiple(app, func, paths=None, method='GET'):
    if paths is None:
        paths = ['/api/2.3/spore']
    app.routes = []
    for path in paths:
        app.route(path, method, func)

def response_spore():
    response.set_cookie("SERVER_ID", ("a7afc9b1fc79e640f2487ba48243ca071c07a823d27"
                                      "8cf9b7adf0fae467a524747e3c6c6973262130fac2b"
                                      "96a11693fa8bd38623e4daee121f60b4301aef012c"))
    return response


def doc_response():
    return response

def proxy_response():
    return response

def proxy_response_402():
    res = response
    res.status = "402 Payment required"
    return res


class TestBridgeWorker(BaseServersTest):

    def test_init(self):
        # setup_routing(self.api_server_bottle, response_spore)
        self.worker = EdrDataBridge(config)
        self.assertEqual(self.worker.delay, 15)
        self.assertEqual(self.worker.increment_step, 1)
        self.assertEqual(self.worker.decrement_step, 1)

        # check clients
        self.assertTrue(isinstance(self.worker.tenders_sync_client, TendersClientSync))
        self.assertTrue(isinstance(self.worker.client, TendersClient))
        self.assertTrue(isinstance(self.worker.proxyClient, ProxyClient))
        self.assertTrue(isinstance(self.worker.doc_service_client, DocServiceClient))

        # check events
        self.assertFalse(self.worker.initialization_event.is_set())
        self.assertTrue(self.worker.until_too_many_requests_event.is_set())

        # check processing items
        self.assertEqual(self.worker.processing_items, {})

    @patch('openprocurement.bot.identification.databridge.bridge.ProxyClient')
    @patch('openprocurement.bot.identification.databridge.bridge.DocServiceClient')
    @patch('openprocurement.bot.identification.databridge.bridge.TendersClientSync')
    @patch('openprocurement.bot.identification.databridge.bridge.TendersClient')
    def test_tender_sync_clients(self, sync_client, client, doc_service_client, proxy_client):
        self.worker = EdrDataBridge(config)
        # check client config
        self.assertEqual(client.call_args[0], ('',))
        self.assertEqual(client.call_args[1], {'host_url': config['main']['public_tenders_api_server'],
                                               'api_version': config['main']['tenders_api_version']})

        # check sync client config
        self.assertEqual(sync_client.call_args[0], (config['main']['api_token'],))
        self.assertEqual(sync_client.call_args[1],
                         {'host_url': config['main']['tenders_api_server'],
                          'api_version': config['main']['tenders_api_version']})

        # check doc_service_client config
        self.assertEqual(doc_service_client.call_args[1],
                         {'host': config['main']['doc_service_server'],
                          'port': config['main']['doc_service_port'],
                          'user': config['main']['doc_service_user'],
                          'password': config['main']['doc_service_password']})
        # check proxy_client config
        self.assertEqual(proxy_client.call_args[1],
                         {'host': config['main']['proxy_server'],
                          'port': config['main']['proxy_port'],
                          'user': config['main']['proxy_user'],
                          'password': config['main']['proxy_password'],
                          'version': config['main']['proxy_version']})

    def test_start_jobs(self):
        # setup_routing(self.api_server_bottle, response_spore)
        self.worker = EdrDataBridge(config)

        scanner, filter_tender, edr_handler, upload_file = [MagicMock(return_value=i) for i in range(4)]
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file

        self.worker._start_jobs()
        # check that all jobs were started
        self.assertTrue(scanner.called)
        self.assertTrue(scanner.called)
        self.assertTrue(scanner.called)
        self.assertTrue(scanner.called)

        self.assertEqual(self.worker.jobs['scanner'], 0)
        self.assertEqual(self.worker.jobs['filter_tender'], 1)
        self.assertEqual(self.worker.jobs['edr_handler'], 2)
        self.assertEqual(self.worker.jobs['upload_file'], 3)

    @patch('gevent.sleep')
    def test_run(self, sleep):
        self.worker = EdrDataBridge(config)
        # create mocks
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file

        with patch('__builtin__.True', AlmostAlwaysTrue(100)):
            self.worker.run()
        self.assertEqual(scanner.call_count, 1)
        self.assertEqual(filter_tender.call_count, 1)
        self.assertEqual(edr_handler.call_count, 1)
        self.assertEqual(upload_file.call_count, 1)

    # region testing checks of separate services

    def test_proxy_server_failure(self):
        self.proxy_server.stop()
        self.worker = EdrDataBridge(config)
        with self.assertRaises(RequestException):
            self.worker.check_proxy()
        self.proxy_server.start()
        self.assertEqual(self.worker.check_proxy(), True)

    def test_proxy_server_success(self):
        self.worker = EdrDataBridge(config)
        self.assertEqual(self.worker.check_proxy(), True)

    def test_doc_service_failure(self):
        self.doc_server.stop()
        self.worker = EdrDataBridge(config)
        with self.assertRaises(RequestError):
            self.worker.check_doc_service()
        self.doc_server.start()
        self.assertEqual(self.worker.check_doc_service(), True)

    def test_doc_service_success(self):
        self.worker = EdrDataBridge(config)
        self.assertEqual(self.worker.check_doc_service(), True)

    def test_api_server_failure(self):
        self.worker = EdrDataBridge(config)
        self.api_server.stop()
        with self.assertRaises(RequestError) as err_context:
            self.worker.check_openprocurement_api()
        self.assertEqual(err_context.exception.message, "socket.error: [Errno 111] Connection refused")
        self.api_server.start()
        self.assertEqual(self.worker.check_openprocurement_api(), True)

    def test_api_server_success(self):
        self.worker = EdrDataBridge(config)
        self.assertEqual(self.worker.check_openprocurement_api(), True)

    def test_paid_requests_failure(self):
        setup_routing(self.proxy_server_bottle, proxy_response_402, path='/api/1.0/details/{}'.format(test_x_edr_internal_id))
        self.worker = EdrDataBridge(config)
        with self.assertRaises(RequestException) as err:
            self.assertEqual(self.worker.check_paid_requests(), True)
        self.assertEqual(err.exception.message, "Not enough money")
        setup_routing(self.proxy_server_bottle, proxy_response, path='/api/1.0/details/{}'.format(test_x_edr_internal_id))
        self.assertEqual(self.worker.check_paid_requests(), True)

    def test_paid_requests_success(self):
        setup_routing(self.proxy_server_bottle, proxy_response, path='/api/1.0/details/{}'.format(test_x_edr_internal_id))
        self.worker = EdrDataBridge(config)
        self.assertEqual(self.worker.check_paid_requests(), True)

    # endregion

    # region check_and_stop

    def test_check_and_stop_did_not_stop(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker.check_proxy = MagicMock()
        self.worker.check_proxy.return_value = True
        self.worker.check_doc_service = MagicMock()
        self.worker.check_doc_service.return_value = True
        self.worker.check_openprocurement_api = MagicMock()
        self.worker.check_openprocurement_api.return_value = True
        self.worker._start_jobs()
        self.assertEqual(self.worker.is_sleeping, False)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, False)
        has_stopped = self.worker.check_services_and_stop()
        self.assertEqual(self.worker.check_proxy.call_count, 1)
        self.assertEqual(self.worker.check_doc_service.call_count, 1)
        self.assertEqual(self.worker.check_openprocurement_api.call_count, 1)
        self.assertEqual(has_stopped, False)
        self.assertEqual(self.worker.is_sleeping, False)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, False)

    def test_check_and_stop_stops_after_exit(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        temp = self.worker.set_exit_status
        self.worker.set_exit_status = MagicMock(side_effect=temp)
        self.assertEqual(self.worker.set_exit_status.call_count, 0)
        self.worker._start_jobs()
        self.assertEqual(self.worker.is_sleeping, False)
        has_stopped = self.worker.check_services_and_stop()
        self.assertEqual(self.worker.set_exit_status.call_count, 0)
        self.assertEqual(has_stopped, False)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, False)
        scanner.return_value.exit = True
        has_stopped = self.worker.check_services_and_stop()
        self.assertEqual(self.worker.set_exit_status.call_count, 1)
        self.assertEqual(has_stopped, True)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)
        self.worker.set_exit_status.assert_called_with(True)
        self.assertEqual(self.worker.set_exit_status.call_count, 1)

    def test_check_and_stop_not_stops_mock_jobs(self):
        self.worker = EdrDataBridge(config)
        temp = self.worker.set_exit_status
        self.worker.set_exit_status = MagicMock(side_effect=temp)
        self.assertEqual(self.worker.set_exit_status.call_count, 0)
        self.worker.is_sleeping = False
        mock_job = MagicMock()
        self.worker.jobs = {'test': mock_job}

        mock_job.exit = False
        has_stopped = self.worker.check_services_and_stop()
        self.assertEqual(has_stopped, False)
        self.assertEqual(self.worker.is_sleeping, False)
        self.assertEqual(self.worker.set_exit_status.call_count, 0)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, False)

    def test_check_and_stop_stops_after_exit_mock_jobs(self):
        self.worker = EdrDataBridge(config)
        temp = self.worker.set_exit_status
        self.worker.set_exit_status = MagicMock(side_effect=temp)
        self.assertEqual(self.worker.set_exit_status.call_count, 0)
        self.worker.is_sleeping = False
        mock_job = MagicMock()
        self.worker.jobs = {'test': mock_job}

        mock_job.exit = True
        has_stopped = self.worker.check_services_and_stop()
        self.assertEqual(has_stopped, True)
        self.assertEqual(self.worker.is_sleeping, True)
        self.assertEqual(self.worker.set_exit_status.call_count, 1)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.worker.set_exit_status.assert_called_with(True)

    def test_check_and_stop_stops_with_failed_proxy(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker._start_jobs()
        self.assertEqual(self.worker.check_services_and_stop(), False)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, False)
        self.assertEqual(self.worker.is_sleeping, False)
        setup_routing(self.proxy_server_bottle, proxy_response_402, path='/api/1.0/health')
        self.assertEqual(self.worker.check_services_and_stop(), True)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)
        setup_routing(self.proxy_server_bottle, proxy_response, path='/api/1.0/health')

    def test_check_and_stop_stops_with_failed_doc_service(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker._start_jobs()
        has_stopped = self.worker.check_services_and_stop()
        self.assertEqual(has_stopped, False)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, False)
        self.assertEqual(self.worker.is_sleeping, False)
        self.doc_server.stop()
        has_stopped = self.worker.check_services_and_stop()
        self.assertEqual(has_stopped, True)
        self.assertEqual(self.worker.is_sleeping, True)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.doc_server.start()

    def test_check_and_stop_stops_with_failed_api(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker._start_jobs()
        has_stopped = self.worker.check_services_and_stop()
        self.assertEqual(has_stopped, False)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, False)
        self.assertEqual(self.worker.is_sleeping, False)
        self.api_server.stop()
        has_stopped = self.worker.check_services_and_stop()
        self.assertEqual(has_stopped, True)
        self.assertEqual(self.worker.is_sleeping, True)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.api_server.start()

    # endregion

    # region check_and_start

    def test_check_and_start_call(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker._start_jobs()
        self.worker.set_exit_status(True)
        setup_routing_multiple(self.proxy_server_bottle, proxy_response,
                               paths=['/api/1.0/health', '/api/1.0/details/{}'.format(test_x_edr_internal_id)])
        setup_routing(self.proxy_server_bottle, proxy_response, path='/api/1.0/details/{}'.format(test_x_edr_internal_id))
        self.worker.check_services_and_start()
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, False)
        self.assertEqual(self.worker.is_sleeping, False)

    def test_check_and_start_does_not_start_with_failed_proxy(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker._start_jobs()
        # setup_routing(self.proxy_server_bottle, proxy_response_402, path='/api/1.0/health')
        self.proxy_server.stop()
        self.worker.set_exit_status(True)
        self.worker.is_sleeping = True
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)
        self.assertEqual(self.worker.check_services_and_start(), "Services are still unavailable")
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)
        self.proxy_server.start()

    def test_check_and_start_does_not_start_without_paid_requests(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker._start_jobs()
        setup_routing(self.proxy_server_bottle, proxy_response_402, path='/api/1.0/details/{}'.format(test_x_edr_internal_id))
        self.worker.set_exit_status(True)
        self.worker.is_sleeping = True
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)
        self.assertEqual(self.worker.check_services_and_start(), "Services are still unavailable")
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)

    def test_check_and_start_does_not_start_with_failed_doc_service(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker._start_jobs()
        self.doc_server.stop()
        self.worker.set_exit_status(True)
        self.worker.is_sleeping = True
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)
        self.assertEqual(self.worker.check_services_and_start(), "Services are still unavailable")
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)
        self.doc_server.start()

    def test_check_and_start_does_not_start_with_failed_api(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker._start_jobs()
        self.api_server.stop()
        self.worker.set_exit_status(True)
        self.worker.is_sleeping = True
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)
        self.assertEqual(self.worker.check_services_and_start(), "Services are still unavailable")
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)
        self.api_server.start()

    def test_check_and_start_starts_with_all_up(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker.check_proxy = MagicMock()
        self.worker.check_proxy.return_value = True
        self.worker.check_paid_requests = MagicMock()
        self.worker.check_paid_requests.return_value = True
        self.worker.check_doc_service = MagicMock()
        self.worker.check_doc_service.return_value = True
        self.worker.check_openprocurement_api = MagicMock()
        self.worker.check_openprocurement_api.return_value = True
        self.worker._start_jobs()
        self.worker.set_exit_status(True)
        self.worker.is_sleeping = True
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)
        temp = self.worker.set_exit_status
        self.worker.set_exit_status = MagicMock(side_effect=temp)
        self.assertEqual(self.worker.check_services_and_start(), "All services have become available, starting all workers")
        self.worker.set_exit_status.assert_called_with(False)
        self.assertEqual(self.worker.set_exit_status.call_count, 1)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, False)
        self.assertEqual(self.worker.is_sleeping, False)

    def test_check_and_start_does_not_start_with_failed_api_then_starts(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker._start_jobs()
        self.api_server.stop()
        self.worker.set_exit_status(True)
        self.worker.is_sleeping = True
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)
        self.assertEqual(self.worker.check_services_and_start(), "Services are still unavailable")
        self.api_server.start()
        self.assertEqual(self.worker.check_services_and_start(), "All services have become available, starting all workers")
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, False)
        self.assertEqual(self.worker.is_sleeping, False)

    def test_check_and_start_starts_after_stop(self):
        """
            1. Configure worker, set up mocks of all 4 workers with exit parameters
            2. Start all jobs
            3. Simulate that /health now returns an error
            4. Check that check_services_and_stop stops all workers and sets is_sleeping to True
            5. Simulate return of /health to normal
            6. Check that check_services_and_start starts all workers and sets is_sleeping to False
        """
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker._start_jobs()
        setup_routing(self.proxy_server_bottle, proxy_response_402, path='/api/1.0/health')
        self.worker.check_services_and_stop()
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)
        setup_routing_multiple(self.proxy_server_bottle, proxy_response,
                               paths=['/api/1.0/health', '/api/1.0/details/{}'.format(test_x_edr_internal_id)])
        setup_routing(self.proxy_server_bottle, proxy_response, path='/api/1.0/details/{}'.format(test_x_edr_internal_id))
        self.worker.check_services_and_start()
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, False)
        self.assertEqual(self.worker.is_sleeping, False)

    def test_check_and_start_does_not_start_after_stop(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker._start_jobs()
        setup_routing(self.proxy_server_bottle, proxy_response_402, path='/api/1.0/health')
        self.worker.check_services_and_stop()
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)
        self.worker.check_services_and_start()
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.assertEqual(self.worker.is_sleeping, True)

    # endregion

    def test_combine_check_stop_and_start_api(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker._start_jobs()
        has_stopped = self.worker.check_services_and_stop()
        self.assertEqual(has_stopped, False)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, False)
        self.assertEqual(self.worker.is_sleeping, False)
        self.api_server.stop()
        has_stopped = self.worker.check_services_and_stop()
        self.assertEqual(has_stopped, True)
        self.assertEqual(self.worker.is_sleeping, True)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.api_server.start()
        has_started = self.worker.check_services_and_start()
        self.assertEqual(has_started, "All services have become available, starting all workers")
        has_stopped = self.worker.check_services_and_stop()
        self.assertEqual(has_stopped, False)

    def test_set_exit(self):
        self.worker = EdrDataBridge(config)
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        scanner.return_value.exit = False
        filter_tender.return_value.exit = False
        edr_handler.return_value.exit = False
        upload_file.return_value.exit = False
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        self.worker._start_jobs()
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, False)
        self.worker.set_exit_status(True)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, True)
        self.worker.set_exit_status(False)
        for i in self.worker.jobs.values():
            self.assertEqual(i.exit, False)

    @patch('gevent.sleep')
    def test_run_mock_exception(self, sleep):
        self.worker = EdrDataBridge(config)
        # create mocks
        scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file
        temp = self.worker.check_services_and_stop
        self.worker.check_services_and_stop = MagicMock(side_effect=Exception)
        with patch('__builtin__.True', AlmostAlwaysTrue(100)):
            self.worker.run()
        self.assertEqual(scanner.call_count, 1)
        self.assertEqual(filter_tender.call_count, 1)
        self.assertEqual(edr_handler.call_count, 1)
        self.assertEqual(upload_file.call_count, 1)

    # # @patch('gevent.sleep')
    # def test_run_mock_check_services_and_start(self):
    #     self.worker = EdrDataBridge(config)
    #     self.assertEqual(self.worker.delay, 0.1)
    #     # create mocks
    #     scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
    #     self.worker.scanner = scanner
    #     self.worker.filter_tender = filter_tender
    #     self.worker.edr_handler = edr_handler
    #     self.worker.upload_file = upload_file
    #     temp = self.worker.check_services_and_stop
    #     self.worker.check_services_and_start = MagicMock()
    #     self.worker.is_sleeping = True
    #     class Mock_Sleep()
    #     with
    #     self.worker.run()
    #     self.assertEqual(self.worker.check_services_and_start.call_count, 1)
    #     self.assertEqual(scanner.call_count, 1)
    #     self.assertEqual(filter_tender.call_count, 1)
    #     self.assertEqual(edr_handler.call_count, 1)
    #     self.assertEqual(upload_file.call_count, 1)



    # @patch('gevent.sleep')
    # def test_run_mock_keyboard_interrupt(self, sleep):
    #     self.worker = EdrDataBridge(config)
    #     # create mocks
    #     scanner, filter_tender, edr_handler, upload_file = [MagicMock() for i in range(4)]
    #     self.worker.scanner = scanner
    #     self.worker.filter_tender = filter_tender
    #     self.worker.edr_handler = edr_handler
    #     self.worker.upload_file = upload_file
    #     temp = self.worker.check_services_and_stop
    #     self.worker.check_services_and_stop = MagicMock(side_effect=KeyboardInterrupt)
    #     with patch("openprocurement.bot.identification.databridge.bridge.gevent.killall"):
    #         self.worker.run()
    #     self.assertEqual(scanner.call_count, 1)
    #     self.assertEqual(filter_tender.call_count, 1)
    #     self.assertEqual(edr_handler.call_count, 1)
    #     self.assertEqual(upload_file.call_count, 1)


