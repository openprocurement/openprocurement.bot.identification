# -*- coding: utf-8 -*-
from gevent import sleep as gsleep


def custom_sleep(seconds):
    return gsleep(seconds=0)
