# The primary objects

from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column, column_property
from sqlalchemy import DateTime, ForeignKeyConstraint, create_engine, String, ForeignKey, select
import pandas as pd


class Base(MappedAsDataclass, DeclarativeBase):
    """subclasses will be converted to dataclasses"""


class Blocks(Base):
    __tablename__ = "blocks"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    datetime: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)


class Transactions(Base):
    __tablename__ = "transactions"
    tx_hash: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(ForeignKey("blocks.block"), nullable=False)
    chain_id: Mapped[int] = mapped_column(ForeignKey("blocks.chain_id"), nullable=False)

    from_address: Mapped[int] = mapped_column(nullable=False)
    to_address: Mapped[int] = mapped_column(nullable=False)
    effective_gas_price: Mapped[int] = mapped_column(nullable=False)
    gas_used: Mapped[int] = mapped_column(nullable=False)
    # defined as gas_used * effective_gas_price
    gas_cost_in_eth: Mapped[float] = mapped_column(nullable=False)
    label: Mapped[str] = mapped_column(nullable=True)  # what kind of transaction this is (solver, rebalance etc?)


class Tokens(Base):
    __tablename__ = "tokens"

    address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    reference_asset: Mapped[str] = mapped_column(nullable=True)  # ETH? USDC? pxETH? None, for CRV / BAL


class Autopools(Base):
    __tablename__ = "autopools"
    vault_address: Mapped[str] = mapped_column(primary_key=True)

    block_deployed: Mapped[int] = mapped_column(ForeignKey("blocks.block"), nullable=False)
    chain_id: Mapped[int] = mapped_column(ForeignKey("blocks.chain_id"), nullable=False)

    name: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)
    strategy_address: Mapped[str] = mapped_column(nullable=True)
    # not certain if the strategy address can be changed
    base_asset: Mapped[str] = mapped_column(nullable=False)


class Destinations(Base):
    __tablename__ = "destinations"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)

    block_deployed: Mapped[int] = mapped_column(ForeignKey("blocks.block"), nullable=False)
    chain_id: Mapped[int] = mapped_column(ForeignKey("blocks.chain_id"), nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)
    exchange_name: Mapped[str] = mapped_column(nullable=False)
    pool: Mapped[str] = mapped_column(nullable=False)
    underlying: Mapped[str] = mapped_column(nullable=False)

    # not certain here on how to connect it to the 
    underlying_tokens: Mapped[list[str]] = mapped_column(nullable=False)


