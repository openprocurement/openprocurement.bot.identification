# coding=utf-8
import os
import subprocess
import unittest

from time import sleep

from bottle import Bottle, response, request
from gevent.pywsgi import WSGIServer
from nose import tools
from redis import StrictRedis

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
            'cache_db_name': 0,
            'cache_host': '127.0.0.1',
            'cache_port': '16379',
            'time_to_live': 1800,
            'delay': 1,
            'time_to_live_negative': 120
        }
}


@tools.nottest
class BaseServersTest(unittest.TestCase):
    """Api server to test openprocurement.integrations.edr.databridge.bridge """
    relative_to = os.path.dirname(__file__)  # crafty line

    @classmethod
    def setUpClass(cls):
        cls.api_server_bottle = Bottle()
        cls.proxy_server_bottle = Bottle()
        cls.doc_server_bottle = Bottle()
        cls.api_server = WSGIServer(('127.0.0.1', 20604), cls.api_server_bottle, log=None)
        setup_routing(cls.api_server_bottle, response_spore)
        cls.public_api_server = WSGIServer(('127.0.0.1', 20605), cls.api_server_bottle, log=None)
        cls.doc_server = WSGIServer(('127.0.0.1', 20606), cls.doc_server_bottle, log=None)
        setup_routing(cls.doc_server_bottle, doc_response, path='/')
        cls.proxy_server = WSGIServer(('127.0.0.1', 20607), cls.proxy_server_bottle, log=None)
        setup_routing(cls.proxy_server_bottle, proxy_response, path='/api/1.0/health')
        cls.redis_process = subprocess.Popen(
            ['redis-server', '--port', str(config['main']['cache_port']), '--logfile /dev/null'])
        sleep(0.1)
        cls.redis = StrictRedis(port=str(config['main']['cache_port']))

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
        cls.redis_process.terminate()
        cls.redis_process.wait()
        del cls.api_server_bottle
        del cls.proxy_server_bottle
        del cls.doc_server_bottle

    def tearDown(self):
        del self.worker
        self.redis.flushall()


def setup_routing(app, func, path='/api/{}/spore'.format(config['main']['tenders_api_version']), method='GET'):
    app.route(path, method, func)


def response_spore():
    response.set_cookie("SERVER_ID", ("a7afc9b1fc79e640f2487ba48243ca071c07a823d27"
                                      "8cf9b7adf0fae467a524747e3c6c6973262130fac2b"
                                      "96a11693fa8bd38623e4daee121f60b4301aef012c"))
    return response


def doc_response():
    return response


def proxy_response():
    if request.headers.get("sandbox-mode") != "True":  # Imitation of health comparison
        response.status = 400
    return response
