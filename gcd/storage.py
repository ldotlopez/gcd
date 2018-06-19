from gcd import (
    Packet,
    consts,
    utils
)


import builtins
import dbm
import hashlib
import json
import os
import pickle
import re
import shutil
import tempfile


import apistar


class StorageAPI:
    def __init__(self, datadir):
        os.makedirs(datadir, exist_ok=True)
        os.makedirs(datadir + "/attachments", exist_ok=True)

        self.attachments = datadir + "/attachments"
        self.db = dbm.open(datadir + "/db", "c")

    def isaid(self, aid):
        return re.match('^[0-9a-f]$', aid)

    def list(self, namespace=''):
        keys = (x.decode('utf-8') for x in self.db.keys())

        if not namespace:
            tmp = (x.split('.')[0] for x in keys)

        else:
            Packet.validate_key(namespace)
            ns_len = len(namespace) + 1
            tmp = (x[ns_len:] for x in keys if x.startswith(namespace+'.'))

        return list(set(tmp))

    def get(self, key):
        return self.backlog(key)[0]

    def save(self, packet):
        try:
            packets = self.backlog(packet.key)
        except KeyError:
            packets = []

        _packet = Packet(packet.key, packet.payload)
        for (name, fh) in packet.attachments.items():
            aid = self.write(fh)
            _packet.attachments[name] = aid

        packets.insert(0, _packet)
        dump = pickle.dumps(packets)

        self.db[packet.key] = dump
        return _packet

    def query(self, key, **params):
        return []

    def backlog(self, key, start=0, end=100):
        dbdump = self.db[key]
        packets = pickle.loads(dbdump)

        return packets[start:end]

    def open(self, aid, flags='rb'):
        dest = "{d}/{a}/{a}{b}/{f}".format(
            d=self.attachments,
            a=aid[0],
            b=aid[1],
            f=aid)

        return open(dest, flags)

    def read(self, aid):
        with self.open(aid) as fh:
            return fh.read()

    def write(self, fh):
        digest = hashlib.sha1()

        tf = tempfile.NamedTemporaryFile(delete=False)
        while True:
            buff = fh.read(4*1024*1024)
            if not buff:
                break

            digest.update(buff)
            tf.write(buff)
        tf.close()

        aid = digest.hexdigest()
        dest = "{d}/{a}/{a}{b}".format(
            d=self.attachments,
            a=aid[0],
            b=aid[1])
        os.makedirs(dest, exist_ok=True)
        dest = dest + "/" + aid

        shutil.move(tf.name, dest)
        return aid


class StorageServer(apistar.App):
    def __init__(self, storage, *args, **kwargs):
        routes = [
            apistar.Route(
                '/packets',
                method='GET', handler=self.children, name='list_rootns'),
            apistar.Route(
                '/packet/{key}',
                method='GET', handler=self.get, name='get_packet'),
            apistar.Route(
                '/packet/{key}',
                method='POST', handler=self.save, name='save_packet'),
            apistar.Route(
                '/packet/{ns}/children',
                method='GET', handler=self.children, name='list_packet_children'),
            apistar.Route(
                '/packet/{key}/backlog',
                method='GET', handler=self.backlog, name='packet_backlog'),
            apistar.Route(
                '/attachment/{aid}',
                method='GET', handler=self.attachment, name='get_attachment'),
        ]
        super().__init__(*args, routes=routes, **kwargs)
        self.storage = storage

    def serialize(self, packet):
        return {
            'key': packet.key,
            'payload': packet.payload,
            'timestamp': str(packet.timestamp),
            'attachments': {
                name: self.reverse_url('get_attachment', aid=aid)
                for (name, aid) in packet.attachments.items()
            }
        }

    def get(self, key) -> dict:
        packet = self.storage.get(key)
        return self.serialize(packet)

    def save(self, key,
             content_type: apistar.http.Header,
             data: apistar.http.RequestData) -> dict:

        def load_as_json():
            return Packet(key, data)

        def load_as_multipart():
            attachments = {
                name.split(':')[1]: f.stream
                for (name, f) in data.items()
                if name.startswith('attachment:')
            }
            payload = data["payload"]
            if not isinstance(payload, str):
                payload = payload.read()

            payload = json.loads(payload)
            return Packet(key, payload, attachments=attachments)

        content_type = content_type.split(';')[0]

        if content_type == 'application/json':
            pckt = load_as_json()
        elif content_type == 'multipart/form-data':
            pckt = load_as_multipart()
        else:
            raise NotImplementedError()

        pckt = self.storage.save(pckt)
        return self.serialize(pckt)

    @utils.unroll
    def children(self, ns='') -> list:
        for key in self.storage.list(ns):
            yield {
                'key': key,
                'uri': self.reverse_url('get_packet', key=key)
            }

    def get(self, key) -> dict:
        return self.storage.get(key).asdict()

    def query(self) -> list:
        return []

    @utils.unroll
    def backlog(self, key) -> list:
        for packet in self.storage.backlog(key):
            yield {
                'payload': packet.payload,
                'timestamp': str(packet.timestamp),
            }

    def attachment(self, aid):
        return

def main():
    import argparse
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument('--storage', required=True)
    parser.add_argument('--host', default=consts.DEFAULT_STORAGE_HOST)
    parser.add_argument('--port', default=consts.DEFAULT_STORAGE_PORT)

    args = parser.parse_args(sys.argv[1:])

    storage = StorageAPI(args.storage)
    StorageServer(storage).serve(args.host, args.port, debug=True)


if __name__ == '__main__':
    main()
