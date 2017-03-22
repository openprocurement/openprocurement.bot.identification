# -*- coding: utf-8 -*-
import json
import io

from uuid import uuid4
from collections import namedtuple


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


def create_file(details):
    """ Return temp file with details """
    temporary_file = io.BytesIO()
    temporary_file.name = 'edr_request.json'
    temporary_file.write(json.dumps(details, indent=4, separators=(',', ': '), ensure_ascii=False).encode('utf-8'))
    temporary_file.seek(0)

    return temporary_file

