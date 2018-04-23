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


from gcd import (
    TagError,
    storage
)


Base = declarative.declarative_base()


def create_engine(uri='sqlite:///:memory:', echo=False):
    engine = sqlalchemy.create_engine(uri, echo=echo)

    if uri.startswith('sqlite:///'):
        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(conn, record):
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    return engine


def create_session(uri=None, engine=None, echo=False):
    if not engine:
        engine = create_engine(uri=uri, echo=echo)

    sess = orm.sessionmaker()
    sess.configure(bind=engine)
    Base.metadata.create_all(engine)
    return sess()


class Message(Base):
    __tablename__ = 'message'
    id = Column(Integer, primary_key=True)
    timestamp = Column(TIMESTAMP, default=datetime.now, nullable=False)
    tag = Column(String(256), nullable=False)
    value = Column(BLOB, nullable=True)


class Storage(storage.Storage):
    def __init__(self, dburi):
        self.db = create_session(dburi)

    def _query_set_for_tag(self, tag):
        qs = self.db.query(Message)
        qs = qs.filter(Message.tag == tag)
        qs = qs.order_by(Message.timestamp.desc())
        return qs

    def save(self, tag, value):
        value = json.dumps(value).encode('utf-8')
        message = Message(tag=tag, value=value)
        self.db.add(message)
        self.db.commit()

    def get(self, tag):
        msg = self._query_set_for_tag(tag).first()
        if msg is None:
            raise TagError(tag)

        return json.loads(msg.value.decode('utf-8'))

    def log(self, tag):
        qs = self._query_set_for_tag(tag)
        if qs.count() == 0:
            raise TagError(tag)

        yield from (
            json.loads(msg.value.decode('utf-8'))
            for msg in qs
        )
