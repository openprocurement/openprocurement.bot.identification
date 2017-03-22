# -*- coding: utf-8 -*-
from uuid import uuid4
from collections import namedtuple

id_passport_len = 9

Data = namedtuple('Data', [
    'tender_id',  # tender ID
    'item_id',  # qualification or award ID
    'code',  # EDRPOU, IPN or passport
    'item_name',  # "qualifications" or "awards"
    'edr_ids',  # list of unique identifications in EDR
    'file_content'  # details for file
])


def journal_context(record={}, params={}):
    for k, v in params.items():
        record["JOURNAL_" + k] = v
    return record


def generate_req_id():
    return b'edr-api-data-bridge-req-' + str(uuid4()).encode('ascii')


def validate_param(code):
    return 'code' if code.isdigit() and len(code) != id_passport_len else 'passport'


class RetryException(Exception):
    pass
