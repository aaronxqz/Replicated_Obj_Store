from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WriteOpType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PUT: _ClassVar[WriteOpType]
    DELETE: _ClassVar[WriteOpType]
    UPDATE: _ClassVar[WriteOpType]
    RESET: _ClassVar[WriteOpType]
PUT: WriteOpType
DELETE: WriteOpType
UPDATE: WriteOpType
RESET: WriteOpType

class WriteOp(_message.Message):
    __slots__ = ("type", "key", "value")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    type: WriteOpType
    key: str
    value: bytes
    def __init__(self, type: _Optional[_Union[WriteOpType, str]] = ..., key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...

class PutRequest(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: bytes
    def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...

class GetRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class DeleteRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class UpdateRequest(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: bytes
    def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...

class ListEntry(_message.Message):
    __slots__ = ("key", "size_bytes")
    KEY_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    key: str
    size_bytes: int
    def __init__(self, key: _Optional[str] = ..., size_bytes: _Optional[int] = ...) -> None: ...

class ListResponse(_message.Message):
    __slots__ = ("entries",)
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[ListEntry]
    def __init__(self, entries: _Optional[_Iterable[_Union[ListEntry, _Mapping]]] = ...) -> None: ...

class GetResponse(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class StatsResponse(_message.Message):
    __slots__ = ("live_objects", "total_bytes", "puts", "gets", "deletes", "updates")
    LIVE_OBJECTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_BYTES_FIELD_NUMBER: _ClassVar[int]
    PUTS_FIELD_NUMBER: _ClassVar[int]
    GETS_FIELD_NUMBER: _ClassVar[int]
    DELETES_FIELD_NUMBER: _ClassVar[int]
    UPDATES_FIELD_NUMBER: _ClassVar[int]
    live_objects: int
    total_bytes: int
    puts: int
    gets: int
    deletes: int
    updates: int
    def __init__(self, live_objects: _Optional[int] = ..., total_bytes: _Optional[int] = ..., puts: _Optional[int] = ..., gets: _Optional[int] = ..., deletes: _Optional[int] = ..., updates: _Optional[int] = ...) -> None: ...
