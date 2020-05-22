import abc
import uuid
import typing
from dateutil import parser
from dataclasses import asdict

from shop import events
from shop.events import EventStream, Event
from utils.exceptions import ConcurrentStreamWriteError


class EventStore(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def load_stream(self, aggregate_uuid: uuid.UUID) -> EventStream:
        pass

    @abc.abstractmethod
    def append_to_stream(self,
                         events: typing.List[Event],
                         expected_version: typing.Optional[int],
                         aggregate_uuid: uuid.UUID,
                         ):
        pass


class RethinkDbEventStore(EventStore):

    AGGREGATES_TABLE_NAME = 'aggregates'
    EVENTS_TABLE_NAME = 'events'

    def __init__(self, db, connection):
        self._db = db
        self._conn = connection

    def load_stream(self, aggregate_uuid: uuid.UUID) -> EventStream:
        events_documents = self._db.table(self.EVENTS_TABLE_NAME).filter(
            {'aggregate_id': str(aggregate_uuid)}
        ).eq_join(
            'aggregate_id', self._db.table(self.AGGREGATES_TABLE_NAME)
        ).zip().order_by(self._db.asc('created_at')).run(self._conn)
        listed_events = list(events_documents)
        if not listed_events:
            version = 0
            event_objects = []
        else:
            version = listed_events[0]['version']
            event_objects = [
                self._translate_to_object(raw_event)
                for raw_event in listed_events
            ]

        return EventStream(events=event_objects, version=version)

    def append_to_stream(
        self,
        events: typing.List[Event],
        expected_version: typing.Optional[int],
        aggregate_uuid: uuid.UUID,
    ):

        str_aggregate_id = str(aggregate_uuid)
        events_to_be_inserted = []
        for event in events:
            event_dict = asdict(event)
            event_dict.update(dict(
                aggregate_id=str_aggregate_id,
                created_at=self._db.now().to_iso8601(),
                version=expected_version + 1 if expected_version else 1,
            ))
            events_to_be_inserted.append(event_dict)

        if expected_version is None:
            insert = self._db.table(self.AGGREGATES_TABLE_NAME).insert({
                'id': str_aggregate_id,
                'version': 1
            })
            branch = insert.do(
                lambda result: self._db.branch(
                    result['inserted'] == 1,
                    self._db.table(self.EVENTS_TABLE_NAME).insert(events_to_be_inserted),
                    {'inserted': 0}
                )
            )
            operation_result = branch.run(self._conn)
        else:
            operation_result = self._db.table(self.AGGREGATES_TABLE_NAME).get(
                str_aggregate_id
            ).update({
                'version': self._db.branch(
                    self._db.row['version'].eq(expected_version),
                    expected_version + 1,
                    self._db.row['version']
                )
            }).do(
                lambda result: self._db.branch(
                    result['replaced'] == 1,
                    self._db.table(self.EVENTS_TABLE_NAME).insert(events_to_be_inserted),
                    {'inserted': 0}
                )
            ).run(self._conn)

            if operation_result['inserted'] != len(events_to_be_inserted):
                raise ConcurrentStreamWriteError

        return operation_result

    def _translate_to_object(self, event_document: dict) -> Event:
        event_document.pop('id')
        event_document.pop('aggregate_id')
        event_document.pop('version')

        event_document['created_at'] = parser.parse(event_document['created_at'])

        class_name = event_document.pop('name')
        kwargs = event_document

        event_class: typing.Type[Event] = getattr(events, class_name)
        return event_class(**event_document)
