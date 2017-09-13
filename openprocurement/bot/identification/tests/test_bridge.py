# -*- coding: utf-8 -*-
import os
from pickle import dumps

from mock import patch, MagicMock
from openprocurement.bot.identification.client import DocServiceClient, ProxyClient
from openprocurement.bot.identification.databridge.bridge import EdrDataBridge
from openprocurement.bot.identification.databridge.data import Data
from openprocurement.bot.identification.tests.base import BaseServersTest, config
from openprocurement.bot.identification.tests.utils import custom_sleep, AlmostAlwaysTrue
from openprocurement_client.client import TendersClientSync, TendersClient
from requests import RequestException
from restkit import RequestError


class TestBridgeWorker(BaseServersTest):
    __test__ = True

    def test_init(self):
        self.worker = EdrDataBridge(config)
        self.assertEqual(self.worker.delay, config['main']['delay'])
        self.assertEqual(self.worker.sleep_change_value.time_between_requests, 0)
        self.assertTrue(isinstance(self.worker.tenders_sync_client, TendersClientSync))
        self.assertTrue(isinstance(self.worker.client, TendersClient))
        self.assertTrue(isinstance(self.worker.proxy_client, ProxyClient))
        self.assertTrue(isinstance(self.worker.doc_service_client, DocServiceClient))
        self.assertFalse(self.worker.initialization_event.is_set())
        self.assertEqual(self.worker.process_tracker.processing_items, {})
        self.assertEqual(self.worker.db._backend, "redis")
        self.assertEqual(self.worker.db._db_name, 0)
        self.assertEqual(self.worker.db._port, "16379")
        self.assertEqual(self.worker.db._host, "127.0.0.1")

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
        self.worker = EdrDataBridge(config)

        scanner, filter_tender, edr_handler, upload_file_to_doc_service, upload_file_to_tender = \
            [MagicMock(return_value=i) for i in range(5)]
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file_to_doc_service = upload_file_to_doc_service
        self.worker.upload_file_to_tender = upload_file_to_tender

        self.worker._start_jobs()
        # check that all jobs were started
        self.assertTrue(scanner.called)
        self.assertTrue(filter_tender.called)
        self.assertTrue(edr_handler.called)
        self.assertTrue(upload_file_to_doc_service.called)
        self.assertTrue(upload_file_to_tender.called)

        self.assertEqual(self.worker.jobs['scanner'], 0)
        self.assertEqual(self.worker.jobs['filter_tender'], 1)
        self.assertEqual(self.worker.jobs['edr_handler'], 2)
        self.assertEqual(self.worker.jobs['upload_file_to_doc_service'], 3)
        self.assertEqual(self.worker.jobs['upload_file_to_tender'], 4)

    @patch('gevent.sleep')
    def test_bridge_run(self, sleep):
        self.worker = EdrDataBridge(config)
        # create mocks
        scanner, filter_tender, edr_handler, upload_file_to_doc_service, upload_file_to_tender = \
            [MagicMock() for _ in range(5)]
        self.worker.scanner = scanner
        self.worker.filter_tender = filter_tender
        self.worker.edr_handler = edr_handler
        self.worker.upload_file_to_doc_service = upload_file_to_doc_service
        self.worker.upload_file_to_tender = upload_file_to_tender
        with patch('__builtin__.True', AlmostAlwaysTrue()):
            self.worker.run()
        self.assertEqual(self.worker.scanner.call_count, 1)
        self.assertEqual(self.worker.filter_tender.call_count, 1)
        self.assertEqual(self.worker.edr_handler.call_count, 1)
        self.assertEqual(self.worker.upload_file_to_doc_service.call_count, 1)
        self.assertEqual(self.worker.upload_file_to_tender.call_count, 1)

    def test_proxy_server(self):
        self.worker = EdrDataBridge(config)
        self.worker.sandbox_mode = "True"
        self.proxy_server.stop()
        with self.assertRaises(RequestException):
            self.worker.check_proxy()
        self.proxy_server.start()
        self.assertTrue(self.worker.check_proxy())

    def test_proxy_server_mock(self):
        self.worker = EdrDataBridge(config)
        self.worker.proxy_client = MagicMock(health=MagicMock(side_effect=RequestError()))
        with self.assertRaises(RequestError):
            self.worker.check_proxy()
        self.worker.proxy_client = MagicMock(return_value=True)
        self.assertTrue(self.worker.check_proxy())

    def test_proxy_server_success(self):
        self.worker = EdrDataBridge(config)
        self.worker.sandbox_mode = "True"
        self.assertTrue(self.worker.check_proxy())

    def test_proxy_sandmox_mismatch(self):
        self.worker = EdrDataBridge(config)
        self.worker.sandbox_mode = "False"
        with self.assertRaises(RequestException):
            self.worker.check_proxy()
        self.worker.sandbox_mode = "True"
        self.assertTrue(self.worker.check_proxy())

    def test_doc_service(self):
        self.doc_server.stop()
        self.worker = EdrDataBridge(config)
        with self.assertRaises(RequestError):
            self.worker.check_doc_service()
        self.doc_server.start()
        self.assertTrue(self.worker.check_doc_service())

    def test_doc_service_mock(self):
        self.worker = EdrDataBridge(config)
        with patch("openprocurement.bot.identification.databridge.bridge.request", side_effect=RequestError()):
            with self.assertRaises(RequestError):
                self.worker.check_doc_service()
        with patch("openprocurement.bot.identification.databridge.bridge.request", return_value=True):
            self.assertTrue(self.worker.check_doc_service())

    def test_openprocurement_api_failure(self):
        self.worker = EdrDataBridge(config)
        self.api_server.stop()
        with self.assertRaises(RequestError):
            self.worker.check_openprocurement_api()
        self.api_server.start()
        self.assertTrue(self.worker.check_openprocurement_api())

    def test_openprocurement_api_mock(self):
        self.worker = EdrDataBridge(config)
        self.worker.client = MagicMock(head=MagicMock(side_effect=RequestError()))
        with self.assertRaises(RequestError):
            self.worker.check_openprocurement_api()
        self.worker.client = MagicMock()
        self.assertTrue(self.worker.check_openprocurement_api())

    def test_check_services(self):
        t = os.environ.get("SANDBOX_MODE", "False")
        os.environ["SANDBOX_MODE"] = "True"
        self.worker = EdrDataBridge(config)
        self.worker.services_not_available = MagicMock(set=MagicMock(), clear=MagicMock())
        self.proxy_server.stop()
        self.worker.check_services()
        self.assertTrue(self.worker.services_not_available.clear.called)
        self.proxy_server.start()
        self.worker.check_services()
        self.assertTrue(self.worker.services_not_available.set.called)
        os.environ["SANDBOX_MODE"] = t

    def test_check_services_mock(self):
        self.worker = EdrDataBridge(config)
        self.worker.check_proxy = self.worker.check_openprocurement_api = self.worker.check_doc_service = MagicMock()
        self.worker.set_wake_up = MagicMock()
        self.worker.set_sleep = MagicMock()
        self.worker.check_services()
        self.assertTrue(self.worker.set_wake_up.called)
        self.worker.check_doc_service = MagicMock(side_effect=RequestError())
        self.worker.check_services()
        self.assertTrue(self.worker.set_sleep.called)

    @patch("gevent.sleep")
    def test_check_log(self, gevent_sleep):
        gevent_sleep = custom_sleep
        self.worker = EdrDataBridge(config)
        self.worker.edrpou_codes_queue = MagicMock(qsize=MagicMock(side_effect=Exception()))
        self.worker.check_services = MagicMock(return_value=True)
        self.worker.run()
        self.assertTrue(self.worker.edrpou_codes_queue.qsize.called)

    @patch("gevent.sleep")
    def test_launch(self, gevent_sleep):
        self.worker = EdrDataBridge(config)
        self.worker.run = MagicMock()
        self.worker.all_available = MagicMock(return_value=True)
        self.worker.launch()
        self.worker.run.assert_called_once()

    @patch("gevent.sleep")
    def test_launch_unavailable(self, gevent_sleep):
        self.worker = EdrDataBridge(config)
        self.worker.all_available = MagicMock(return_value=False)
        with patch('__builtin__.True', AlmostAlwaysTrue()):
            self.worker.launch()
        gevent_sleep.assert_called_once()

    def test_unprocessed_items(self):
        data = Data('1', '2', '123', 'awards', {'meta': {'id': '333'}, 'test_data': 'test_data'})
        self.redis.set("unprocessed_{}".format(data.doc_id()), dumps(data))
        self.worker = EdrDataBridge(config)
        self.assertEqual(self.worker.upload_to_doc_service_queue.get(), data)

    def test_check_and_revive_jobs(self):
        self.worker = EdrDataBridge(config)
        self.worker.jobs = {"test": MagicMock(dead=MagicMock(return_value=True))}
        self.worker.revive_job = MagicMock()
        self.worker.check_and_revive_jobs()
        self.worker.revive_job.assert_called_once_with("test")

    def test_revive_job(self):
        self.worker = EdrDataBridge(config)
        self.worker.test = MagicMock()
        self.worker.jobs = {"test": MagicMock(dead=MagicMock(return_value=True))}
        self.worker.revive_job("test")
        self.assertEqual(self.worker.jobs['test'].dead, False)
