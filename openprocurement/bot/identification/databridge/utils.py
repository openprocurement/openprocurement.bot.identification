# -*- coding: utf-8 -*-
import yaml
import io

from uuid import uuid4
from caching import db_key
from logging import getLogger
from datetime import datetime
from restkit import ResourceError

from openprocurement.bot.identification.databridge.constants import file_name

LOGGER = getLogger(__name__)

id_passport_len = 9


class ProcessTracker(object):

    def __init__(self, db=None, ttl=300):
        self.processing_items = {}
        self.processed_items = {}
        self._db = db
        self.tender_documents_to_process = {}
        self.ttl = ttl

    def set_item(self, tender_id, item_id, docs_amount=0):
        self.processing_items[item_key(tender_id, item_id)] = docs_amount
        self.add_docs_amount_to_tender(tender_id, docs_amount)

    def add_docs_amount_to_tender(self, tender_id, docs_amount):
        if self.tender_documents_to_process.get(tender_id):
            self.tender_documents_to_process[tender_id] += docs_amount
        else:
            self.tender_documents_to_process[tender_id] = docs_amount

    def remove_docs_amount_from_tender(self, tender_id):
        if self.tender_documents_to_process[tender_id] > 1:
            self.tender_documents_to_process[tender_id] -= 1
        else:
            self._db.put(db_key(tender_id), datetime.now().isoformat(), self.ttl)
            del self.tender_documents_to_process[tender_id]

    def check_processing_item(self, tender_id, item_id):
        """Check if current tender_id, item_id is processing"""
        return item_key(tender_id, item_id) in self.processing_items.keys()

    def check_processed_item(self, tender_id, item_id):
        """Check if current tender_id, item_id was already processed"""
        return item_key(tender_id, item_id) in self.processed_items.keys()

    def check_processed_tenders(self, tender_id):
        return self._db.has(db_key(tender_id)) or False

    def update_processing_items(self, tender_id, item_id):
        key = item_key(tender_id, item_id)
        if self.processing_items[key] > 1:
            self.processing_items[key] -= 1
        else:
            self.processed_items[key] = datetime.now()
            del self.processing_items[key]

    def update_items_and_tender(self, tender_id, item_id):
        self.update_processing_items(tender_id, item_id)
        self.remove_docs_amount_from_tender(tender_id)


def item_key(tender_id, item_id):
    return '{}_{}'.format(tender_id, item_id)


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


def journal_context(record={}, params={}):
    for k, v in params.items():
        record["JOURNAL_" + k] = v
    return record


def generate_req_id():
    return b'edr-api-data-bridge-req-' + str(uuid4()).encode('ascii')


def generate_doc_id():
    return uuid4().hex


def create_file(details):
    """ Return temp file with details """
    temporary_file = io.BytesIO()
    temporary_file.name = file_name
    temporary_file.write(yaml.safe_dump(details, allow_unicode=True, default_flow_style=False))
    temporary_file.seek(0)

    return temporary_file


class RetryException(Exception):
    pass


def check_add_suffix(list_ids, document_id, number):
    """Check if EDR API returns list of edr ids with more then 1 element add suffix to document id"""
    len_list_ids = len(list_ids)
    if len_list_ids > 1:
        return '{document_id}.{amount}.{number}'.format(document_id=document_id, amount=len_list_ids, number=number)
    return document_id


def check_412(func):
    def func_wrapper(obj, *args, **kwargs):
        try:
            response = func(obj, *args, **kwargs)
        except ResourceError as re:
            if re.status_int == 412:
                obj.headers['Cookie'] = re.response.headers['Set-Cookie']
                response = func(obj, *args, **kwargs)
            else:
                raise ResourceError(re)
        return response

    return func_wrapper
