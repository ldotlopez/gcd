import unittest


import io
import json
import tempfile


from apistar.test import TestClient as APIStarTestClient


from gcd import (
    Client,
    Packet,
    StorageAPI,
    StorageServer
)


# def save(client, path, payload, attachments=None):
#     kwargs = {}

#     if attachments:
#         kwargs['files'] = {
#             'attachment:' + k: v
#             for (k, v) in attachments.items()
#         }
#         kwargs['files']['payload'] = json.dumps(payload)
#     else:
#         kwargs['json'] = payload

#     return client.post(path, **kwargs)


# def read(client, path, *args, **kwargs):
#     resp = client.get(path, *args, **kwargs)
#     return resp, resp.status_code, json.loads(resp.content.decode('utf-8'))


# def save2(client, key, payload, attachments=None):
#     kwargs = {}

#     if attachments:
#         kwargs['files'] = {
#             'attachment:' + k: v
#             for (k, v) in attachments.items()
#         }
#         kwargs['files']['payload'] = json.dumps(payload)
#     else:
#         kwargs['json'] = payload

#     resp = client.post('/packet/' + key, **kwargs)
#     return resp, resp.status_code, json.loads(resp.content.decode('utf-8'))


class StorageTests:
    def read_attachments(self, packet):
        for (name, x) in packet.attachments.items():
            if isinstance(x, str):  # aid
                packet.attachments[name] = self.storage.read(x)

            elif callable(getattr(x, 'read', None)):
                packet.attachments[name].seek(0)
                packet.attachments[name] = packet.attachments[name].read(0)

            else:
                raise NotImplementedError()

    def test_get_complex(self):
        packet = self.storage.save(Packet('x', 1))
        self.assertEqual(packet.payload, 1)
        self.assertEqual(self.storage.get('x').payload, 1)

    def test_get_complex(self):
        payload = [
            '1', 2, False, {'foo': 'bar', 'a': {'b': 'c'}}, 1.8
        ]

        self.storage.save(Packet('x', payload))
        self.assertEqual(self.storage.get('x').payload, payload)

    def test_get_missing(self):
        with self.assertRaises(KeyError) as ctx:
            self.storage.get('x')

    def test_invalid_keys(self):
        with self.assertRaises(TypeError):
            self.storage.save(Packet(1, 2))

        with self.assertRaises(ValueError):
            self.storage.save(Packet('', ''))

        with self.assertRaises(ValueError):
            self.storage.save(Packet('foo..x', ''))

        with self.assertRaises(ValueError):
            self.storage.save(Packet('foo.', ''))

        with self.assertRaises(ValueError):
            self.storage.save(Packet('.foo', ''))

        with self.assertRaises(ValueError):
            self.storage.save(Packet('ñ', ''))

    def test_root_children(self):
        self.storage.save(Packet('x', None))
        self.storage.save(Packet('y', None))

        children = self.storage.list()
        self.assertTrue(
            set(children),
            set(['x', 'y'])
        )

    def test_sub_children(self):
        self.storage.save(Packet('ns.foo', None))
        self.storage.save(Packet('ns.bar', None))

        children = self.storage.list()
        self.assertTrue(
            set(children),
            set(['foo', 'bar'])
        )

    def test_backlog(self):
        for x in range(3):
            self.storage.save(Packet('foo', x))

        backlog = self.storage.backlog('foo')
        payloads = [x.payload for x in backlog]
        self.assertEqual(
            payloads,
            [2, 1, 0]
        )

    def test_backlog_for_missing(self):
        with self.assertRaises(KeyError) as ctx:
            self.storage.backlog('x')


    def test_attachments(self):
        contents = 'hi!'.encode('utf-8')

        p = Packet('foo', None,
                   attachments={'stdout': io.BytesIO(contents)})
        self.storage.save(p)

        p = self.storage.get('foo')
        with self.storage.open(p.attachments['stdout']) as fh:
            buff = fh.read()
            self.assertEqual(buff, contents)


    def test_attachments_backlog(self):
        # Save some packets
        packets = [
            Packet('foo', None, attachments={'x': io.BytesIO(str(idx).encode('utf-8'))})
            for idx in range(3)
        ]
        for packet in packets:
            self.storage.save(packet)

        for (expected, packet) in zip(packets, reversed(self.storage.backlog('foo'))):
            # Read attachments from storage engine
            for (name, aid) in packet.attachments.items():
                with self.storage.open(aid) as fh:
                    packet.attachments[name] = fh.read()

            # Read attachments from original packets
            for (name, aid) in expected.attachments.items():
                expected.attachments[name].seek(0)
                expected.attachments[name] = expected.attachments[name].read()

            # Compare
            self.assertEqual(expected.payload, packet.payload)
            self.assertEqual(expected.attachments, packet.attachments)


class TestDBMStorage(StorageTests, unittest.TestCase):
    def setUp(self):
        d = tempfile.mkdtemp()
        self.storage = StorageAPI(datadir=d)


# class TestStorageServer(unittest.TestCase):
#     def setUp(self):
#         d = tempfile.mkdtemp()
#         storage = StorageAPI(datadir=d)
#         server = StorageServer(storage=storage)
#         self.client = APIStarTestClient(server)

#     # save / get
#     def test_set_with_int(self):
#         resp, code, data = save2(self.client, 'foo', 1)
#         import ipdb; ipdb.set_trace(); pass

#     def test_get_with_attachments(self):
#         with open(__file__) as fh:
#             resp, code, data = save2(
#                 self.client, 'foo', None,
#                 attachments={'me': fh}
#             )

#         resp, code, data = get2(self.client, 'foo')
#         self.assertTrue()

#     def test_get_missing(self):
#         with self.assertRaises(KeyError) as ctx:
#             read(self.client, '/packet/x')

#     def test_save_json(self):
#         save(self.client, '/packet/foo', 1)

#         resp, code, data = read(self.client, '/packet/foo')
#         self.assertEqual(code, 200)
#         self.assertEqual(data['payload'], 1)

#     def test_save_json_complex(self):
#         payload = {'check': True, 'foo': 'bar'}
#         save(self.client, '/packet/foo-complex', payload)

#         resp, code, data = read(self.client, '/packet/foo-complex')
#         self.assertEqual(code, 200)
#         self.assertEqual(data['payload'], payload)

#     def test_save_multipart(self):
#         with open(__file__, 'rb') as fh:
#             save(self.client, '/packet/with-files', {'a': 'b'},
#                  attachments={'me': fh})

#     #
#     # Backlog
#     #
#     def test_backlog(self):
#         save(self.client, '/packet/x', 1)
#         save(self.client, '/packet/x', 2)
#         save(self.client, '/packet/x', 3)

#         resp, code, data = read(self.client, '/packet/x/backlog')
#         payloads = [x['payload'] for x in data]

#         self.assertEqual(payloads, [3, 2, 1])

#     def test_backlog_single(self):
#         save(self.client, '/packet/x', 1)

#         resp, code, data = read(self.client, '/packet/x/backlog')
#         payloads = [x['payload'] for x in data]

#         self.assertEqual(payloads, [1])

#     #
#     # Children
#     #
#     def test_list_root_ns(self):
#         save(self.client, '/packet/a', 1)
#         save(self.client, '/packet/b', 2)

#         resp, code, data = read(self.client, '/packets')
#         data = [x['key'] for x in data]
#         self.assertEqual(set(['a', 'b']), set(data))

#     def test_empty_root_ns(self):
#         resp, code, data = read(self.client, '/packets')
#         data = [x['key'] for x in data]
#         self.assertEqual(data, [])

#     def test_list_key_children(self):
#         save(self.client, '/packet/foo.a', 1)
#         save(self.client, '/packet/foo.b', 2)

#         resp, code, data = read(self.client, '/packet/foo/children')
#         data = [x['key'] for x in data]
#         self.assertEqual(set(['a', 'b']), set(data))


from gcd import Client


class TestClient(Client):
    def __init__(self, app):
        super().__init__()
        self.app = app

    def get_session(self):
        return APIStarTestClient(self.app)

    def request(self, method, path, *args, **kwargs):
        return self.session.request(method, '/packet/' + path, **kwargs)

class FooTest(unittest.TestCase):
    def setUp(self):
        d = tempfile.mkdtemp()
        self.storage = StorageAPI(datadir=d)
        self.server = StorageServer(storage=self.storage)
        self.client = TestClient(self.server)

    def test_set(self):
        packet = self.client.save('foo', 1)
        self.assertEqual(packet.payload, 1)

if __name__ == '__main__':
    unittest.main()
