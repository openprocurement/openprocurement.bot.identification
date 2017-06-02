# -*- coding: utf-8 -*-

import unittest
import os

from requests import RequestException

from mock import patch, MagicMock
from restkit import RequestError

from gevent.pywsgi import WSGIServer
from bottle import Bottle, response

from openprocurement.bot.identification.databridge.bridge import EdrDataBridge
from openprocurement_client.client import TendersClientSync, TendersClient
from openprocurement.bot.identification.client import DocServiceClient, ProxyClient
from openprocurement.bot.identification.databridge.constants import test_x_edr_internal_id




config = {
    'main':
        {
            'tenders_api_server': 'http://127.0.0.1:20604',
            'tenders_api_version': '0',
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
            'api_token': "api_token"
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

    @classmethod
    def tearDownClass(cls):
        cls.api_server.close()
        cls.public_api_server.close()
        cls.doc_server.close()
        cls.proxy_server.close()

    def setUp(self):
        setup_routing(self.proxy_server_bottle, proxy_response, path='/api/1.0/details/{}'.format(test_x_edr_internal_id))
        self.worker = EdrDataBridge(config)
        workers = {'scanner': MagicMock(return_value=MagicMock(exit=False)),
                   'filter_tender': MagicMock(return_value=MagicMock(exit=False)),
                   'edr_handler': MagicMock(return_value=MagicMock(exit=False)),
                   'upload_file': MagicMock(return_value=MagicMock(exit=False))}
        for name, value in workers.items():
            setattr(self.worker, name, value)

    def tearDown(self):
        del self.worker

def setup_routing(app, func, path='/api/0/spore', method='GET'):
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
    response.status = "402 Payment required"
    return response


class TestBridgeWorker(BaseServersTest):

    def test_init(self):
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
        setup_routing(self.api_server_bottle, response_spore)
        self.worker = EdrDataBridge(config)

        scanner, filter_tender, edr_handler, upload_file = [MagicMock(return_value=i) for i in range(4)]
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file = upload_file

        self.worker._start_jobs()
        # check that all jobs were started
        self.assertTrue(scanner.called)
        self.assertTrue(filter_tender.called)
        self.assertTrue(edr_handler.called)
        self.assertTrue(upload_file.called)

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
        self.assertEqual(self.worker.scanner.call_count, 1)
        self.assertEqual(self.worker.filter_tender.call_count, 1)
        self.assertEqual(self.worker.edr_handler.call_count, 1)
        self.assertEqual(self.worker.upload_file.call_count, 1)

    def test_proxy_server(self):
        self.proxy_server.stop()
        with self.assertRaises(RequestException):
            self.worker.check_proxy()
        self.proxy_server.start()
        self.assertEqual(self.worker.check_proxy(), True)

    def test_doc_service(self):
        self.doc_server.stop()
        with self.assertRaises(RequestError):
            self.worker.check_doc_service()
        self.doc_server.start()
        self.assertEqual(self.worker.check_doc_service(), True)

    def test_api_server(self):
        self.api_server.stop()
        with self.assertRaises(RequestError):
            self.worker.check_openprocurement_api()
        self.api_server.start()
        self.assertEqual(self.worker.check_openprocurement_api(), True)

    def test_paid_requests(self):
        setup_routing(self.proxy_server_bottle, proxy_response_402, path='/api/1.0/details/{}'.format(test_x_edr_internal_id))
        with self.assertRaises(RequestException):
            self.assertEqual(self.worker.check_paid_requests(), True)
        setup_routing(self.proxy_server_bottle, proxy_response, path='/api/1.0/details/{}'.format(test_x_edr_internal_id))
        self.assertEqual(self.worker.check_paid_requests(), True)

    def test_check_and_stop_did_not_stop(self):
        self.worker._start_jobs()
        functions = {'check_proxy': MagicMock(return_value = True),
                     'check_doc_service': MagicMock(return_value = True),
                     'check_openprocurement_api': MagicMock(return_value = True)}
        for name, value in functions.items():
            setattr(self.worker, name, value)
        self.worker.check_services_and_stop()
        self.assertEqual(all([i.call_count == 1 for i in functions.values()]), True)
        self.assertEqual(self.worker.is_sleeping, False)
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), False)

    def test_check_and_stop(self):
        self.worker._start_jobs()
        self.proxy_server.stop()
        self.worker.check_services_and_stop()
        self.assertEqual(self.worker.is_sleeping, True)
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), True)
        self.proxy_server.start()
        self.worker.set_sleep(False)

        self.doc_server.stop()
        self.worker.check_services_and_stop()
        self.assertEqual(self.worker.is_sleeping, True)
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), True)
        self.doc_server.start()
        self.worker.set_sleep(False)

        self.api_server.stop()
        self.worker.check_services_and_stop()
        self.assertEqual(self.worker.is_sleeping, True)
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), True)
        self.api_server.start()
        self.worker.set_sleep(False)

        self.worker.scanner.return_value.exit = True
        self.worker.check_services_and_stop()
        self.assertEqual(self.worker.is_sleeping, True)
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), True)

    def test_check_and_start_does_not_start(self):
        self.worker._start_jobs()
        self.proxy_server.stop()
        self.worker.set_sleep(True)
        self.worker.check_services_and_start()
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), True)
        self.assertEqual(self.worker.is_sleeping, True)
        self.proxy_server.start()
        self.worker.check_services_and_start()
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), False)

        self.doc_server.stop()
        self.worker.set_sleep(True)
        self.worker.check_services_and_start()
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), True)
        self.assertEqual(self.worker.is_sleeping, True)
        self.doc_server.start()
        self.worker.check_services_and_start()
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), False)

        self.api_server.stop()
        self.worker.set_sleep(True)
        self.worker.check_services_and_start()
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), True)
        self.assertEqual(self.worker.is_sleeping, True)
        self.api_server.start()
        self.worker.check_services_and_start()
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), False)

        setup_routing(self.proxy_server_bottle, proxy_response_402, path='/api/1.0/details/{}'.format(test_x_edr_internal_id))
        self.worker.set_sleep(True)
        self.worker.check_services_and_start()
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), True)
        self.assertEqual(self.worker.is_sleeping, True)
        setup_routing(self.proxy_server_bottle, proxy_response, path='/api/1.0/details/{}'.format(test_x_edr_internal_id))
        self.worker.check_services_and_start()
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), False)

    def test_check_and_start(self):
        self.worker._start_jobs()
        functions = {'check_proxy': MagicMock(return_value = True), 'check_paid_requests': MagicMock(return_value = True),
                     'check_doc_service': MagicMock(return_value = True), 'check_openprocurement_api': MagicMock(return_value = True)}
        for name, value in functions.items():
            setattr(self.worker, name, value)
        self.worker.set_sleep(True)
        self.worker.check_services_and_start()
        self.assertEqual(self.worker.is_sleeping, False)
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), False)

    def test_combine_check_stop_and_start_api(self):
        self.worker._start_jobs()
        self.api_server.stop()
        self.worker.check_services_and_stop()
        self.assertEqual(self.worker.is_sleeping, True)
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), True)
        self.api_server.start()
        self.worker.check_services_and_start()
        self.assertEqual(self.worker.is_sleeping, False)
        self.assertEqual(all([i.exit for i in self.worker.jobs.values()]), False)

    @patch('gevent.sleep')
    def test_run_mock_check_services_and_start(self, sleep):
        self.worker.check_services_and_start = MagicMock()
        self.worker.check_services_and_stop = MagicMock()
        self.worker.is_sleeping = True
        self.worker.run()
        self.assertEqual(self.worker.check_services_and_start.call_count, 1)
        self.assertEqual(self.worker.check_services_and_stop.call_count, 21)
        self.assertEqual(self.worker.scanner.call_count, 1)
        self.assertEqual(self.worker.filter_tender.call_count, 1)
        self.assertEqual(self.worker.edr_handler.call_count, 1)
        self.assertEqual(self.worker.upload_file.call_count, 1)
