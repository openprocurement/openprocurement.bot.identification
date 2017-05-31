# -*- coding: utf-8 -*-
import logging
from gevent import sleep as gsleep
from uuid import uuid4
from json import dumps


def custom_sleep(seconds):
    return gsleep(seconds=0)


def generate_answers(answers, default):
    """ Yield results, or default """
    def answer_generator():
        for i in answers:
            yield i
        while True:
            yield default
    return answer_generator()


def generate_request_id():
    return 'req-{}'.format(uuid4().hex)


class ResponseMock(object):

    def __init__(self, headers, data, status_int=200):
        self.data = data
        self.headers = headers
        self.status_int = status_int

    def body_string(self):
        return dumps(self.data)

    def next(self):
        pass

class MockLoggingHandler(logging.Handler):
    """Mock logging handler to check for expected logs.

    Messages are available from an instance's ``messages`` dict, in order, indexed by
    a lowercase log level string (e.g., 'debug', 'info', etc.).
    """

    def __init__(self, *args, **kwargs):
        self.messages = {'debug': [], 'info': [], 'warning': [], 'error': [],
                         'critical': []}
        super(MockLoggingHandler, self).__init__(*args, **kwargs)

    def emit(self, record):
        "Store a message from ``record`` in the instance's ``messages`` dict."
        self.acquire()
        try:
            self.messages[record.levelname.lower()].append(record.getMessage())
        finally:
            self.release()

    def reset(self):
        self.acquire()
        try:
            for message_list in self.messages.values():
                del message_list[:]
        finally:
            self.release()
