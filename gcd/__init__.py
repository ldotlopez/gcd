import datetime
import enum
import importlib


import blinker


Undefined = object()


class TagError(KeyError):
    pass


class Event(enum.Enum):
    VALUE_CHANGED = 0


class API():
    def __init__(self, storage_backend='default', storage_opts=None):
        if storage_opts is None:
            storage_opts = {}

        mod = 'gcd.storage.' + storage_backend
        mod = importlib.import_module(mod)

        self.storage = mod.Storage(**storage_opts)
        self.events = {ev: blinker.Signal(ev.name) for ev in Event}

    def save(self, tag, value):
        try:
            prev = self.storage.get(tag)
        except TagError:
            prev = Undefined

        timestamp = datetime.datetime.now()
        self.storage.save(tag, value, timestamp)
        if prev != value:
            self.notify(Event.VALUE_CHANGED, value=value, prev=prev)

    def get(self, tag):
        return self.storage.get(tag)

    def log(self, tag):
        g = self.storage.log(tag)

        while True:
            try:
                yield next(g)
            except StopIteration:
                break

    def children(self, tag):
        raise NotImplementedError()

    def connect(self, event, fn):
        self.events[event].connect(fn)

    def disconnect(self, event, fn):
        self.event[event].disconnect(fn)

    def notify(self, event, **kwargs):
        self.events[event].send(self, **kwargs)


class Storage:
    def __init__(self, **storage_opts):
        raise NotImplementedError()

    def save(self, tag, value):
        raise NotImplementedError()

    def get(self, tag):
        raise NotImplementedError()

    def log(self, tag):
        raise NotImplementedError()


class Packet:
    def __init__(self, tag, value, timestamp=None):
        if not isinstance(tag, str) or not tag:
            raise TypeError(tag, "expected not empty string")

        now = datetime.datetime.now()
        if timestamp is None:
            timestamp = now
        else:
            if not isinstance(timestamp, datetime.datetime):
                raise TypeError(timestamp, "expected datetime object")
            if timestamp > now:
                raise ValueError(timestamp, "timestamp is in the future")

        self.tag = tag
        self.value = value
        self.timestamp = timestamp
