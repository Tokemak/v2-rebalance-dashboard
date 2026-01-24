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


class FixedHexBytes(TypeDecorator):
    """
    Stores Addresses, Transaction Hashes and topics as fixed-length byte arrays in the DB.

    This should be faster and more space-efficient than storing as hex strings.

    # not tested
    """

    impl = BYTEA
    cache_ok = True

    def __init__(self, nbytes: int):
        super().__init__()
        self.nbytes = nbytes

    def copy(self, **kw):
        return FixedHexBytes(self.nbytes)

    @property
    def python_type(self):
        return HexBytes

    def process_bind_param(self, value, dialect):
        if value is None:
            return None

        try:
            if isinstance(value, (HexBytes, bytes, bytearray, memoryview)):
                b = bytes(value)
            elif isinstance(value, str):
                v = value[2:] if value.startswith("0x") else value
                b = bytes.fromhex(v)
            else:
                raise TypeError(f"Unsupported type for FixedHexBytes({self.nbytes}): {type(value)}")
        except ValueError as e:
            raise ValueError(f"Invalid hex value for FixedHexBytes({self.nbytes})") from e

        if len(b) != self.nbytes:
            raise ValueError(f"expected {self.nbytes} bytes, got {len(b)}")
        return b

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return HexBytes(value)


EvmAddress = FixedHexBytes(20)
EvmTxHash = FixedHexBytes(32)
EvmTopic = FixedHexBytes(32)