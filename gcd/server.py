import json
import mimeparse
from wsgiref import simple_server


import falcon
import falcon_multipart.middleware


import gcd


class HTTPApp(falcon.API):
    def __init__(self, storage_backend, storage_opts):
        self.api = gcd.API(storage_backend, **storage_opts)

        super().__init__(
            middleware=[falcon_multipart.middleware.MultipartMiddleware()],
        )

        msg_col_rsrc = PacketCollectionResource(api=self.api)
        msg_item_rsrc = PacketItemResource(api=self.api)
        msg_log_rsrc = PacketBacklogCollection(api=self.api)

        self.add_route('/packet', msg_col_rsrc)
        self.add_route('/packet/{tag}', msg_item_rsrc)
        self.add_route('/packet/{tag}/log', msg_log_rsrc)


class PacketCollectionResource:
    def __init__(self, api):
        self.api = api

    def on_post(self, req, resp):
        typ, subtyp, props = mimeparse.parse_mime_type(req.content_type)

        if typ == 'multipart' and subtyp in ['form-data', 'mixed']:
            # Handle multipart uploads

            packet = req.get_param('packet')

            # packet not found in request
            if not packet:
                raise ValueError('Missing packet')

            # Invalid json for packet
            try:
                packet = json.loads(packet)
            except json.decoder.JSONDecodeError as e:
                raise ValueError('Malformed packet') from e

            form_field = req.get_param('attachment')
            attachment = form_field.file.read()

        elif typ == 'application' and subtyp == 'json':
            # Handle simple requests

            # Invalid json for packet
            try:
                packet = json.load(req.stream)
            except json.decoder.JSONDecodeError as e:
                raise ValueError('Malformed packet') from e

            attachment = None

        else:
            raise ValueError('Unknow request')

        tag = packet['tag']
        value = packet['value']

        self.api.save(tag, value)

        resp.status = falcon.HTTP_204


class PacketItemResource:
    def __init__(self, api):
        self.api = api

    def on_get(self, req, resp, tag):
        try:
            value = self.api.get(tag)

        except gcd.TagError:
            resp.status = falcon.HTTP_404
            return

        resp.status = falcon.HTTP_200
        resp.content_type = falcon.MEDIA_JSON
        resp.body = json.dumps(value)


class PacketBacklogCollection:
    def __init__(self, api, max_packets=100):
        self.api = api
        self.limit = max_packets

    def on_get(self, req, resp, tag):
        log = list(self.api.log(tag))

        resp.status = falcon.HTTP_200
        resp.content_type = falcon.MEDIA_JSON
        resp.body = json.dumps([value for value in log])


if __name__ == '__main__':
    app = HTTPApp('default', storage_opts={})
    httpd = simple_server.make_server('127.0.0.1', 8000, app)
    httpd.serve_forever()
