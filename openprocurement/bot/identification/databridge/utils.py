# -*- coding: utf-8 -*-
import io
from copy import deepcopy, copy
from logging import getLogger
from uuid import uuid4

import yaml
from openprocurement.bot.identification.databridge.constants import file_name, version, \
    qualification_procurementMethodType, pre_qualification_procurementMethodType
from restkit import ResourceError
from simplejson import JSONDecodeError

LOGGER = getLogger(__name__)


def item_key(tender_id, item_id):
    return '{}_{}'.format(tender_id, item_id)


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


def get_res_json(response):
    try:
        return response.json()
    except JSONDecodeError:
        return response.text


def is_no_document_in_edr(response, res_json):
    return (response.status_code == 404 and isinstance(res_json, dict)
            and res_json.get('errors')[0].get('description')[0].get('error').get('code') == u"notFound")


def is_payment_required(response):
    return (response.status_code == 403 and response.headers.get('content-type', '') == 'application/json'
            and (response.json().get('errors')[0].get('description') ==
                 [{'message': 'Payment required.', 'code': 5}]))


def fill_data_list(response, tender_data, data_list, process_tracker):
    for i, obj in enumerate(response.json()['data']):
        document_id = check_add_suffix(response.json()['data'], tender_data.doc_id(), i + 1)
        file_content = {'meta': {'sourceDate': response.json()['meta']['detailsSourceDate'][i]}, 'data': obj}
        file_content['meta'].update(deepcopy(tender_data.file_content['meta']))
        file_content['meta'].update({"version": version})
        file_content['meta']['id'] = document_id
        data = copy(tender_data)
        data.file_content = file_content
        process_tracker.add_unprocessed_item(tender_data)
        data_list.append(data)


def is_code_invalid(code):
    return (not (type(code) == int or (type(code) == str and code.isdigit()) or
                 (type(code) == unicode and code.isdigit())))


def item_id(item):
    return item['bidID' if item.get('bidID') else 'bid_id']


def journal_item_name(item):
    return "QUALIFICATION_ID" if item.get('bidID') else "AWARD_ID"


def check_related_lot_status(tender, award):
    """Check if related lot not in status cancelled"""
    lot_id = award.get('lotID')
    if lot_id:
        if [l['status'] for l in tender.get('lots', []) if l['id'] == lot_id][0] != 'active':
            return False
    return True


def journal_item_params(tender, item):
    return {"TENDER_ID": tender['id'], "BID_ID": item_id(item), journal_item_name(item): item['id']}


def more_tenders(params, response):
    return not (params.get('descending')
                and not len(response.data) and params.get('offset') == response.next_page.offset)


def valid_qualification_tender(tender):
    return (tender['status'] == "active.qualification" and
            tender['procurementMethodType'] in qualification_procurementMethodType)


def valid_prequal_tender(tender):
    return (tender['status'] == 'active.pre-qualification' and
            tender['procurementMethodType'] in pre_qualification_procurementMethodType)
