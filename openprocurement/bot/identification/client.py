# -*- coding: utf-8 -*-
import requests
import base64


class DocServiceClient(object):
    """Base class for making requests to Document Service"""

    def __init__(self, host, token, port=6555, timeout=None):
        self.session = requests.Session()
        self.token = base64.b64encode(token)
        self.url = '{host}:{port}/upload'.format(host=host, port=port)
        self.headers = {"Authorization": "Basic {token}".format(token=self.token)}
        self.timeout = timeout

    def upload(self, files):
        files = {'file': files}
        response = self.session.post(url=self.url, headers=self.headers, timeout=self.timeout, files=files)

        return response
