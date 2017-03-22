# -*- coding: utf-8 -*-
import requests


class ProxyClient(object):
    """Base class for making requests to Proxy server"""

    def __init__(self, host, token, timeout=None, port=6547):
        self.session = requests.Session()
        self.token = token
        self.verify_url = '{host}:{port}/verify'.format(host=host, port=port)
        self.details_url = '{host}:{port}/details'.format(host=host, port=port)
        self.headers = {"Authorization": "Basic {token}".format(token=self.token)}
        self.timeout = timeout

    def verify(self, param, code):
        """Send request to Proxy server to verify EDRPOU code"""
        url = '{url}?{param}={code}'.format(url=self.verify_url, param=param, code=code)
        response = self.session.get(url=url, headers=self.headers, timeout=self.timeout)

        return response

    def details(self, id):
        """ Send request to Proxy server to get details."""
        url = '{url}/{id}'.format(url=self.details_url, id=id)
        response = self.session.get(url=url, headers=self.headers, timeout=self.timeout)

        return response
