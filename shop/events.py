import datetime
import typing
from dataclasses import dataclass


@dataclass
class Event:
    pass


@dataclass
class OrderCreated(Event):
    user_id: int
    name: str = 'OrderCreated'
    created_at: datetime = None
    version = None


@dataclass
class OrderStatusChanged(Event):
    new_status: str
    name: str = 'OrderStatusChanged'
    created_at: datetime = None
    version = None


class EventStream:
    events: typing.List[Event]
    version: int

    def __init__(self, events, version):
        self.events = events
        self.version = version
