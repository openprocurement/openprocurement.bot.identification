# coding=utf-8
import pickle

from datetime import datetime

from openprocurement.bot.identification.databridge.caching import db_key
from openprocurement.bot.identification.databridge.utils import item_key


class ProcessTracker(object):
    def __init__(self, db=None, ttl=300):
        self.processing_items = {}
        self.processed_items = {}
        self._db = db
        self.tender_documents_to_process = {}
        self.ttl = ttl

    def set_item(self, tender_id, item_id, docs_amount=0):
        self.processing_items[item_key(tender_id, item_id)] = docs_amount
        self._add_docs_amount_to_tender(tender_id, docs_amount)

    def _add_docs_amount_to_tender(self, tender_id, docs_amount):
        if self.tender_documents_to_process.get(tender_id):
            self.tender_documents_to_process[tender_id] += docs_amount
        else:
            self.tender_documents_to_process[tender_id] = docs_amount

    def _remove_docs_amount_from_tender(self, tender_id):
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

    def get_unprocessed_items(self):
        return self._db.get_items("unprocessed_*") or []

    def add_unprocessed_item(self, data):
        self._db.put(data.doc_id(), pickle.dumps(data), self.ttl)

    def _remove_unprocessed_item(self, document_id):
        self._db.remove(document_id)

    def _update_processing_items(self, tender_id, item_id, document_id):
        key = item_key(tender_id, item_id)
        if self.processing_items[key] > 1:
            self.processing_items[key] -= 1
        else:
            self.processed_items[key] = datetime.now()
            self._remove_unprocessed_item(document_id)
            del self.processing_items[key]

    def update_items_and_tender(self, tender_id, item_id, document_id):
        self._update_processing_items(tender_id, item_id, document_id)
        self._remove_docs_amount_from_tender(tender_id)
