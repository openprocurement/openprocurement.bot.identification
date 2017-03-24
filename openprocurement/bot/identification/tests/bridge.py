# -*- coding: utf-8 -*-

import unittest
import os

from gevent.pywsgi import WSGIServer
from bottle import Bottle, request, response

from openprocurement.bot.identification.databridge.bridge import EdrDataBridge
from openprocurement_client.client import TendersClientSync, TendersClient
from openprocurement.bot.identification.client import DocServiceClient, ProxyClient


config = {
    'main':
        {
            'tenders_api_server': 'http://127.0.0.1:20604',
            'tenders_api_version': '0',
            'public_tenders_api_server': 'http://127.0.0.1:20605',
            'doc_service_server': 'http://127.0.0.1',
            'doc_service_port': 20606,
            'doc_service_token': 'broker:broker',
            'proxy_server': 'http://127.0.0.1',
            'proxy_port': 20607,
            'proxy_token': 'cm9ib3Q6cm9ib3Q=',
            'buffers_size': 450,
            'full_stack_sync_delay': 15,
            'empty_stack_sync_delay': 101,
            'on_error_sleep_delay': 5,
            'api_token': "api_token"
        }
}


class BaseServersTest(unittest.TestCase):
    """Api server to test openprocurement.integrations.edr.databridge.bridge """

    relative_to = os.path.dirname(__file__)  # crafty line

    @classmethod
    def setUpClass(cls):
        cls.api_server_bottle = Bottle()
        cls.proxy_server_bottle = Bottle()
        cls.doc_server_bottle = Bottle()
        cls.api_server = WSGIServer(('127.0.0.1', 20604), cls.api_server_bottle, log=None)
        cls.public_api_server = WSGIServer(('127.0.0.1', 20605), cls.api_server_bottle, log=None)
        cls.doc_server = WSGIServer(('127.0.0.1', 20606), cls.doc_server_bottle, log=None)
        cls.proxy_server = WSGIServer(('127.0.0.1', 20607), cls.proxy_server_bottle, log=None)

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

    def tearDown(self):
        del self.worker


def setup_routing(app, func, path='/api/0/spore', method='GET'):
    app.route(path, method, func)


def response_spore():
    response.set_cookie("SERVER_ID", ("a7afc9b1fc79e640f2487ba48243ca071c07a823d27"
                                      "8cf9b7adf0fae467a524747e3c6c6973262130fac2b"
                                      "96a11693fa8bd38623e4daee121f60b4301aef012c"))
    return response


class TestBridgeWorker(BaseServersTest):

    def test_init(self):
        setup_routing(self.api_server_bottle, response_spore)
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
