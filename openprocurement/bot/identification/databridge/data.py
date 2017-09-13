# coding=utf-8
from openprocurement.bot.identification.databridge.constants import id_passport_len


class Data(object):
    def __init__(self, tender_id, item_id, code, item_name, file_content):
        self.tender_id = tender_id
        self.item_id = item_id
        self.code = code
        self.item_name = item_name
        self.file_content = file_content

    def __eq__(self, other):
        return (self.tender_id == other.tender_id and
                self.item_id == other.item_id and
                self.code == other.code and
                self.item_name == other.item_name and
                self.file_content == other.file_content)

    def __str__(self):
        return "tender {} {} id: {}".format(self.tender_id, self.item_name[:-1], self.item_id)

    def doc_id(self):
        return self.file_content['meta']['id']

    def item_name_id(self):
        return self.item_name[:-1].upper() + "_ID"

    def param(self):
        return 'id' if self.code.isdigit() and len(self.code) != id_passport_len else 'passport'

    def add_unique_req_id(self, response):
        if response.headers.get('X-Request-ID'):
            self.file_content['meta']['sourceRequests'].append(response.headers['X-Request-ID'])

    def log_params(self):
        return {"TENDER_ID": self.tender_id, self.item_name_id(): self.item_id, "DOCUMENT_ID": self.doc_id()}
