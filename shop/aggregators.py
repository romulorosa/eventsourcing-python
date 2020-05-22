import abc
import uuid
from uuid import UUID

import typing

from shop.event_store import EventStream, EventStore
from shop.events import OrderCreated, OrderStatusChanged, Event
from utils.decorators import method_dispatch


class Aggregate(metaclass=abc.ABCMeta):

    def __init__(self, event_stream: EventStream, uuid: UUID):
        self._changes = []
        self._event_stream = event_stream
        self._version = event_stream.version
        self._uuid = uuid

        for event in event_stream.events:
            self.apply(event)
            self._changes = []

    @abc.abstractmethod
    def apply(self, event: Event):
        pass

    @property
    def version(self):
        return self._version

    @property
    def uuid(self):
        return self._uuid

    @property
    def changes(self):
        return self._changes

    @property
    def event_stream(self):
        return self._event_stream

    def set_changes(self, changes: typing.List[Event]):
        self._changes += changes


class Order(Aggregate):

    def __repr__(self):
        return '<Order uuid=%s status=%s version=%s changes=%s' % (
            self.uuid,
            self.status,
            self.version,
            len(self.changes)
        )

    @classmethod
    def create(cls, user_id: int):
        initial_event = OrderCreated(user_id)
        event_stream = EventStream([], None)
        order_instance = cls(event_stream, uuid.uuid1())
        order_instance.apply(initial_event)
        return order_instance

    @method_dispatch
    def _apply(self, event):
        pass

    def apply(self, event: Event):
        if self._apply(event):
            self.set_changes([event])

    @_apply.register(OrderCreated)
    def _(self, event: OrderCreated) -> bool:
        self.user_id = event.user_id
        self.status = 'new'
        return True

    @_apply.register(OrderStatusChanged)
    def _(self, event: OrderStatusChanged) -> bool:
        self.status = event.new_status
        return True


class AggregatesRepository:
    def __init__(self, event_store: EventStore):
        self.event_store = event_store

    def get(self, aggregate_cls: typing.Type[Aggregate], aggregate_uuid: UUID):
        event_stream = self.event_store.load_stream(aggregate_uuid)
        return aggregate_cls(event_stream, aggregate_uuid)

    def save(self, aggregate: Aggregate):
        return self.event_store.append_to_stream(aggregate.changes, aggregate.version, aggregate.uuid)
