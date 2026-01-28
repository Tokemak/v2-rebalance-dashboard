from sqlalchemy.types import TypeDecorator
from sqlalchemy.dialects.postgresql import BYTEA
from hexbytes import HexBytes
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass
from dataclasses import asdict


class Base(MappedAsDataclass, DeclarativeBase):

    def to_record(self) -> dict:
        return asdict(self)

    @classmethod
    def from_record(cls, record: dict):
        valid_cols = {c.name for c in cls.__table__.columns}
        filtered = {k: v for k, v in record.items() if k in valid_cols}
        return cls(**filtered)

    def to_tuple(self) -> tuple:
        """
        Returns a tuple of this instance's column values in the order defined by the table's columns.
        """
        return tuple(getattr(self, c.name) for c in self.__table__.columns)

    @classmethod
    def from_tuple(cls, tup: tuple):
        # returns an instance of this class from the ordered tuple
        col_names = [c.name for c in cls.__table__.columns]
        return cls(**dict(zip(col_names, tup)))
