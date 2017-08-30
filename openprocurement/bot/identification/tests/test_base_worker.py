# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from gevent import event, spawn
from mock import patch, MagicMock

from openprocurement.bot.identification.databridge.base_worker import BaseWorker
from openprocurement.bot.identification.tests.base import BaseServersTest
from openprocurement.bot.identification.tests.utils import custom_sleep, AlmostAlwaysFalse


class TestBaseWorker(BaseServersTest):
    __test__ = True

    def test_init(self):
        services_not_available = event.Event()
        self.worker = BaseWorker(services_not_available)
        self.assertEqual(self.worker.services_not_available, services_not_available)
        self.assertFalse(self.worker.exit)

    def test_start_jobs(self):
        services_not_available = event.Event()
        self.worker = BaseWorker(services_not_available)
        with self.assertRaises(NotImplementedError):
            self.worker._start_jobs()

    def test_check_and_revive_jobs(self):
        self.worker = BaseWorker(MagicMock())
        self.worker.immortal_jobs = {"test": MagicMock(dead=MagicMock(return_value=True))}
        self.worker.revive_job = MagicMock()
        self.worker.check_and_revive_jobs()
        self.worker.revive_job.assert_called_once_with("test")

    @patch('gevent.sleep')
    def test_run(self, sleep):
        sleep = custom_sleep
        self.worker = BaseWorker(MagicMock())
        self.worker._start_jobs = MagicMock(return_value={"test": self.func})
        self.worker.check_and_revive_jobs = MagicMock()
        with patch.object(self.worker, 'exit', AlmostAlwaysFalse()):
            self.worker._run()
        self.worker.check_and_revive_jobs.assert_called_once()

    @patch('gevent.sleep')
    def test_run_exception(self, sleep):
        sleep = custom_sleep
        self.worker = BaseWorker(MagicMock())
        self.worker._start_jobs = MagicMock(return_value={"test": spawn(self.func)})
        self.worker.check_and_revive_jobs = MagicMock(side_effect=Exception)
        with patch.object(self.worker, 'exit', AlmostAlwaysFalse()):
            self.worker._run()
        self.worker.check_and_revive_jobs.assert_called_once()

    def test_shutdown(self):
        self.worker = BaseWorker(MagicMock())
        self.worker.shutdown()
        self.assertTrue(self.worker.exit)

    def func(self):
        pass
