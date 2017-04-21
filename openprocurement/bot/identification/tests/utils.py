# -*- coding: utf-8 -*-
from gevent import sleep as gsleep


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
