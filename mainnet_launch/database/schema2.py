from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column, column_property
from sqlalchemy import DateTime, ForeignKeyConstraint, create_engine, String, ForeignKey, select
from sqlalchemy.dialects.postgresql import ARRAY

import pandas as pd

from dotenv import load_dotenv
from urllib.parse import urlparse
import os

from mainnet_launch.constants import eth_client


load_dotenv()

tmpPostgres = urlparse(os.getenv("DEV_LOCAL_DATABASE_URL"))

engine = create_engine(
    f"postgresql+psycopg2://{tmpPostgres.username}:{tmpPostgres.password}"
    f"@{tmpPostgres.hostname}{tmpPostgres.path}?sslmode=require",
    echo=True,  # Enable SQL query logging for debugging.
)

# docs https://docs.sqlalchemy.org/en/20/orm/dataclasses.html#declarative-dataclass-mapping
# choice to store everything as pd.Timestamp, not as unix timestamps


class Base(MappedAsDataclass, DeclarativeBase):
    """subclasses will be converted to dataclasses"""


class Blocks(Base):
    __tablename__ = "blocks"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    datetime: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)


class GlobalTokenValues:
    # the value of a token indepenedent of a pool, ie at this moment everywhere
    __tablename__ = "global_token_values"

    address: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(ForeignKey("blocks.block"), nullable=False)
    chain_id: Mapped[int] = mapped_column(ForeignKey("blocks.chain_id"), nullable=False)

    denomination: Mapped[str] = mapped_column(
        nullable=True
    )  # what the backing and price are determined in (WETH, USDC)
    backing: Mapped[float] = mapped_column(nullable=True)
    safe_price: Mapped[float] = mapped_column(nullable=True)


class Transactions(Base):
    __tablename__ = "transactions"

    tx_hash: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(ForeignKey("blocks.block"), nullable=False)
    chain_id: Mapped[int] = mapped_column(ForeignKey("blocks.chain_id"), nullable=False)

    tx_from: Mapped[int] = mapped_column(nullable=False)
    tx_to: Mapped[int] = mapped_column(nullable=False)
    effective_gas_price: Mapped[int] = mapped_column(nullable=False)
    gas_used: Mapped[int] = mapped_column(nullable=False)
    # defined as gas_used * effective_gas_price
    gas_cost_in_eth: Mapped[float] = mapped_column(nullable=False)


# constants, (information that does not change over time)
# places on chain


class Tokens(Base):
    __tablename__ = "tokens"

    address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    reference_asset: Mapped[str] = mapped_column(nullable=True)  # ETH? USDC? pxETH? None, for CRV / BAL


class DestinationTokenValue:
    # informtion about a single asset in a single pool.
    # we care aboute how
    __tablename__ = "global_token_values"

    token_address: Mapped[str] = mapped_column(primary_key=True)
    destination_address: Mapped[str] = mapped_column(primary_key=True)
    datetime: Mapped[pd.Timestamp] = mapped_column(primary_key=True)  # forign key
    chain_id: Mapped[int] = mapped_column()

    spot_price: Mapped[float] = mapped_column(nullable=True)
    quantity: Mapped[float] = mapped_column(nullable=True)


class Autopools(Base):  # change name
    __tablename__ = "Autopools"

    __table_args__ = (ForeignKeyConstraint(["block_deployed", "chain_id"], ["blocks.block", "blocks.chain_id"]),)

    block_deployed: Mapped[int] = mapped_column()
    chain_id: Mapped[int] = mapped_column()
    # we can infer the datetime it was deployed by looking
    #  at the select (datetime from block_time where (block=block_deployed and chain_id = chain_id))
    name: Mapped[str] = mapped_column()
    symbol: Mapped[str] = mapped_column()
    vault_address: Mapped[str] = mapped_column(primary_key=True)
    strategy_address: Mapped[str] = mapped_column(nullable=True)
    # not certain if the strategy address can be changed
    base_asset: Mapped[str] = mapped_column()


class AutopoolState(Base):
    __tablename__ = "autopool_state"

    vault_address: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    __table_args__ = (
        # Enforce that vault_address exists in AutopoolConstants.
        ForeignKeyConstraint(
            ["address"],
            ["autopool_constants.vault_address"],
        ),
        # Enforce that (block, chain_id) exists in BlockTime.
        ForeignKeyConstraint(
            ["block", "chain_id"],
            ["block_time.block", "block_time.chain_id"],
        ),
    )

    # the raw (history less) values we can get onchain at this block

    nav: Mapped[float] = mapped_column()
    total_shares: Mapped[float] = mapped_column()
    active_destination_addresses: Mapped[list[str]] = mapped_column(ARRAY(String))

    composite_return_out: Mapped[float] = mapped_column()
    composite_return_in: Mapped[float] = mapped_column()
    price_return: Mapped[float] = mapped_column()
    # othere data


class DestinationConstants(Base):
    __tablename__ = "destination_constants"

    address: Mapped[str] = mapped_column()
    # other immutable thigns about the vault


class DestinationState(Base):
    __tablename__ = "destination_state"

    address: Mapped[str] = mapped_column()  # connected to destination constants

    #


# # Create all tables as defined by the Base metadata.
# # Base.metadata.create_all(engine)
# print("All tables have been created.")


def main():

    data = [
        {"block": 1, "chain_id": 1, "timestamp": pd.to_datetime("2023-04-15 12:00:00", utc=True)},
        {"block": 1, "chain_id": 2, "timestamp": pd.to_datetime("2023-04-15 12:01:00", utc=True)},
    ]

    blocktime_df = pd.DataFrame(data)

    # # what is a simple way to add the new rows to the db
    # blocktime_df.to_sql()


if __name__ == "__main__":
    main()
