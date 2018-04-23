import json
from datetime import datetime


import sqlalchemy
from sqlalchemy import (
    event,
    orm,
    Column,
    BLOB,
    Integer,
    String,
    TIMESTAMP
)
from sqlalchemy.ext import declarative


import gcd


Base = declarative.declarative_base()


class NativePacket(Base):
    __tablename__ = 'packet'
    id = Column(Integer, primary_key=True)
    timestamp = Column(TIMESTAMP, nullable=False)
    tag = Column(String(256), nullable=False)
    value = Column(BLOB, nullable=True)


class Storage(gcd.Storage):
    def __init__(self, dburi):
        self.db = self._create_session(dburi)

    def save(self, packet):
        self.db.add(self._gcd_to_native(packet))
        self.db.commit()

    def get(self, tag):
        native = self._query_set_for_tag(tag).first()
        if native is None:
            raise gcd.TagError(tag)

        return self._native_to_gcd(native)

    def log(self, tag):
        qs = self._query_set_for_tag(tag)
        if qs.count() == 0:
            raise gcd.TagError(tag)

        yield from (self._native_to_gcd(native) for native in qs)

    def _query_set_for_tag(self, tag):
        qs = self.db.query(NativePacket)
        qs = qs.filter(NativePacket.tag == tag)
        qs = qs.order_by(NativePacket.timestamp.desc())
        return qs

    @staticmethod
    def _create_session(uri='sqlite:///:memory:', echo=False):
        engine = sqlalchemy.create_engine(uri, echo=echo)

        if uri.startswith('sqlite:///'):
            @event.listens_for(engine, "connect")
            def set_sqlite_pragma(conn, record):
                cursor = conn.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

        sess = orm.sessionmaker()
        sess.configure(bind=engine)
        Base.metadata.create_all(engine)
        return sess()



    @staticmethod
    def _gcd_to_native(packet):
        return NativePacket(
            tag=packet.tag,
            value=json.dumps(packet.value).encode('utf-8'),
            timestamp=packet.timestamp)

    @staticmethod
    def _native_to_gcd(native):
        return gcd.Packet(
            tag=native.tag,
            value=json.loads(native.value.decode('utf-8')),
            timestamp=native.timestamp)
