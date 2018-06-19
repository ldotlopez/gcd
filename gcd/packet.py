import datetime
import io
import re


class Packet:
    def __init__(self, key, payload, timestamp=None, attachments=None):
        self.validate_key(key)

        if timestamp is None:
            timestamp = datetime.datetime.utcnow()
        else:
            self.validate_timestamp(timestamp)

        if attachments:
            self.validate_attachments(attachments)

        self.key = key
        self.payload = payload
        self.timestamp = timestamp
        self.attachments = attachments or {}

    @classmethod
    def fromdict(cls, d):
        timestamp = d.get('timestamp')
        if timestamp:
            timestamp = datetime.datetime.strptime(timestamp,
                                                   '%Y-%m-%d %H:%M:%S.%f')

        return cls(d['key'], d['payload'], timestamp=timestamp)

    @staticmethod
    def validate_key(key):
        if not isinstance(key, str):
            msg = "key must be a non string"
            raise TypeError(msg)

        if not key:
            msg = "key can't be an empty string"
            raise ValueError(msg)

        if key[0] == '.' or key[-1] == '.':
            msg = "key can't start or end with a dot"
            raise ValueError(msg)

        if '..' in key:
            msg = "key has a double dot"
            raise ValueError(msg)

        if not re.match(r'^[a-z0-9_\-\.]+$', key):
            msg = "key can only contain [a-z0-9], undercores or dashes"
            raise ValueError(msg)

    @staticmethod
    def validate_timestamp(timestamp):
        if not isinstance(timestamp, datetime.datetime):
            raise TypeError(timestamp, "expected datetime object")

        if timestamp > datetime.datetime.utcnow():
            raise ValueError(timestamp, "timestamp is in the future")

    @staticmethod
    def validate_attachments(attachments):
        if not isinstance(attachments, dict):
            raise TypeError(attachments, "expected dict")

        if not all([x and isinstance(x, str) for x in attachments.keys()]):
            raise TypeError(attachments, "expected dict(str->data)")

        for name in attachments:
            if not name or not isinstance(name, str):
                raise TypeError(attachments, "expected dict(str->IOBase)")

            read_meth = getattr(attachments[name], 'read', None)
            if not callable(read_meth):
                raise TypeError(attachments, "expected dict(str->Readable)")

    def asdict(self):
        return {
            'key': self.key,
            'payload': self.payload,
            'timestamp': str(self.timestamp),
        }


class Message(Packet):
    def __init__(self, key, code, text, fmt, items, timestamp=None, attachments=None):
        payload = {
            'code': code,
            'text': text,
            'format': format,
            'items': items
        }

        super().__init__(key, payload, timestamp, attachments)

    @property
    def code(self):
        return self.payload['code']

    @property
    def text(self):
        return self.payload['text']
