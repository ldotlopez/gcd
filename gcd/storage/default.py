import dbm
import pickle
import tempfile
from datetime import datetime


import gcd


class Storage(gcd.Storage):
    def __init__(self, filepath=None):
        if filepath is None:
            base = tempfile.mkdtemp()
            filepath = base + '/gcd'

        self._db = dbm.open(filepath, 'c')

    def _native_get(self, native_tag):
        container = self._db[native_tag]
        container = pickle.loads(container, encoding='utf-8')
        return container

    def _native_save(self, native_tag, container):
        self._db[native_tag] = pickle.dumps(container)

    def save(self, packet):
        native_tag = packet.tag.encode('utf-8')

        try:
            container = self._native_get(native_tag)
        except KeyError:
            container = []

        container.append(packet)
        self._native_save(native_tag, container)

    def get(self, tag):
        native_tag = tag.encode('utf-8')
        try:
            container = self._native_get(native_tag)
        except KeyError as e:
            raise gcd.TagError(tag) from e

        return container[-1]

    def log(self, tag):
        tag_bytes = tag.encode('utf-8')
        try:
            container = self._native_get(tag)
        except KeyError as e:
            raise gcd.TagError(tag) from e

        yield from reversed(container)
