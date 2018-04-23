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

    def _native_get(self, tag):
        container = self._db[tag]
        container = pickle.loads(container, encoding='utf-8')
        return container

    def _native_save(self, tag, container):
        self._db[tag] = pickle.dumps(container)

    def save(self, tag, value, timestamp=None):
        if not timestamp:
            timestamp = datetime.now()

        tag_bytes = tag.encode('utf-8')

        try:
            container = self._native_get(tag_bytes)
        except KeyError:
            container = []

        container.append({
            'tag': tag,
            'value': value,
            'timestamp': timestamp
        })
        self._native_save(tag_bytes, container)

    def get(self, tag):
        tag_bytes = tag.encode('utf-8')
        try:
            container = self._native_get(tag)
        except KeyError as e:
            raise gcd.TagError(tag) from e

        return container[-1]['value']

    def log(self, tag):
        tag_bytes = tag.encode('utf-8')
        try:
            container = self._native_get(tag)
        except KeyError as e:
            raise gcd.TagError(tag) from e

        yield from (
            pack['value']
            for pack in reversed(container)
        )
