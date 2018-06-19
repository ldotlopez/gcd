from gcd import (
    Packet,
    consts
)


import json


import requests


class APIError(Exception):
    pass


class Client:
    def __init__(self, storage_uri=consts.DEFAULT_STORAGE_URI):
        self._session = None
        self.storage_uri = storage_uri

    @property
    def session(self):
        if self._session is None:
            self._session = self.get_session()

        return self._session

    def get_session(self):
        return request.Session()

    def request(self, method, path, *args, **kwargs):
        path = consts.DEFAULT_STORAGE_URI + path
        return self.session.request(method, path, *args, **kwargs)

    def get(self, key):
        resp = self.request('GET', key)
        if resp.status_code != 200:
            raise APIError()

        return Packet.fromdict(json.loads(resp.content.decode('utf-8')))

    def save(self, key, payload, attachments=None):
        kwargs = {}

        if attachments:
            kwargs['files'] = {
                'attachment:' + k: v
                for (k, v) in attachments.items()
            }
            kwargs['files']['payload'] = json.dumps(payload)
        else:
            kwargs['json'] = payload

        resp = self.request('POST', key, **kwargs)
        if resp.status_code != 200:
            raise APIError()

        return Packet.fromdict(json.loads(resp.content.decode('utf-8')))


if __name__ == '__main__':
    pass