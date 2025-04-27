# The primary objects
from dataclasses import asdict
from dotenv import load_dotenv
from urllib.parse import urlparse
import os
import pandas as pd

from sqlalchemy import MetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column
from sqlalchemy import DateTime, ForeignKey, ARRAY, String, ForeignKeyConstraint, BigInteger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# check out a data access layer
load_dotenv()

tmpPostgres = urlparse(os.getenv("DEV_LOCAL_DATABASE_URL"))

ENGINE = create_engine(
    f"postgresql+psycopg2://{tmpPostgres.username}:{tmpPostgres.password}"
    f"@{tmpPostgres.hostname}{tmpPostgres.path}?sslmode=require",
    echo=True,  # Enable SQL query logging for debugging.
)


class Base(MappedAsDataclass, DeclarativeBase):

    def to_record(self) -> dict:
        return asdict(self)

    @classmethod
    def from_record(cls, record: dict):
        return cls(**record)

    @classmethod
    def from_tuple(cls, tup: tuple):
        # returns an instance of this class from the ordered tuple
        col_names = [c.name for c in cls.__table__.columns]
        return cls(**dict(zip(col_names, tup)))

    def to_tuple(self) -> tuple:
        """
        Returns a tuple of this instance's column values in the order defined by the table's columns.
        """
        # Use the table's column order to extract attribute values
        return tuple(getattr(self, c.name) for c in self.__table__.columns)


class Blocks(Base):
    __tablename__ = "blocks"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    datetime: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)


class Transactions(Base):
    __tablename__ = "transactions"

    tx_hash: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(nullable=False)
    chain_id: Mapped[int] = mapped_column(nullable=False)

    from_address: Mapped[str] = mapped_column(String(42), nullable=False)
    to_address: Mapped[str] = mapped_column(String(42), nullable=False)
    effective_gas_price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    gas_used: Mapped[int] = mapped_column(BigInteger, nullable=False)
    gas_cost_in_eth: Mapped[float] = mapped_column(nullable=False)  # gas_used * effective_gas_price

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


# yes, holdable asests, no incentive tokens
class Tokens(Base):
    __tablename__ = "tokens"

    token_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    symbol: Mapped[str] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    decimals: Mapped[int] = mapped_column(nullable=False)


# done
class Autopools(Base):
    __tablename__ = "autopools"
    vault_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    block_deployed: Mapped[int] = mapped_column(nullable=False)  # not certain I care about this

    name: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)

    # not certain if the strategy address can be changed
    strategy_address: Mapped[str] = mapped_column(nullable=True)

    base_asset: Mapped[str] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block_deployed", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


# current
class Destinations(Base):
    __tablename__ = "destinations"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    exchange_name: Mapped[str] = mapped_column(nullable=False)
    block_deployed: Mapped[int] = mapped_column(nullable=False)

    name: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)  # not certain here on if we should have both names and symbols

    pool_type: Mapped[str] = mapped_column(nullable=False)
    pool: Mapped[str] = mapped_column(nullable=False)
    underlying: Mapped[str] = mapped_column(nullable=False)
    underlying_symbol: Mapped[str] = mapped_column(nullable=False)
    underlying_name: Mapped[str] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block_deployed", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


# current
class DestinationTokens(Base):
    __tablename__ = "destination_tokens"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    token_address: Mapped[str] = mapped_column(primary_key=True)
    index: Mapped[int] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["destination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
        ForeignKeyConstraint(
            ["token_address", "chain_id"],
            ["tokens.token_address", "tokens.chain_id"],
        ),
    )


# needed
class AutopoolStates(Base):
    __tablename__ = "autopool_states"

    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    block: Mapped[int] = mapped_column(primary_key=True)

    total_shares: Mapped[float] = mapped_column(nullable=False)
    total_nav: Mapped[float] = mapped_column(nullable=False)
    nav_per_share: Mapped[float] = mapped_column(nullable=False)

    # despends on autopool destination states
    weighted_average_total_apr_out: Mapped[float] = mapped_column(nullable=False)
    weighted_average_total_apr_in: Mapped[float] = mapped_column(nullable=False)
    weighted_average_safe_backing_discount: Mapped[float] = mapped_column(nullable=False)  # price return

    # active_destinations: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(["autopool_vault_address", "chain_id"], ["autopools.vault_address", "autopools.chain_id"]),
    )


# needed
class AutopoolTokenStates(Base):
    __tablename__ = "autopool_token_states"

    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    token_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)

    amount: Mapped[float] = mapped_column(
        nullable=False
    )  # amount of the base asset token (eg WETH, pxETH, not LP tokens)
    total_safe_value: Mapped[float] = mapped_column(nullable=False)
    total_spot_value: Mapped[float] = mapped_column(nullable=False)
    total_backing_value: Mapped[float] = mapped_column(nullable=False)

    # feature does not exist
    # some way of measuing how much of total liquidity is owned by this autopool
    # how much do we get out if we try and sell everything here

    # how good this quote is for
    # dex_aggregator_init_datetime: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)
    # dex_aggregator_cutoff_datetime: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)

    # dex_aggregator_quote_to_base_asset_1_percent: Mapped[float] = mapped_column(nullable=False)
    # dex_aggregator_quote_to_base_asset_10_percent: Mapped[float] = mapped_column(nullable=False)
    # dex_aggregator_quote_to_base_asset_33_percent: Mapped[float] = mapped_column(nullable=False)
    # dex_aggregator_quote_to_base_asset_50_percent: Mapped[float] = mapped_column(nullable=False)
    # dex_aggregator_quote_to_base_asset_100_percent: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["autopool_vault_address", "chain_id"], ["autopools.vault_address", "autopools.chain_id"]),
    )


# extra
class AutopoolDeposit(Base):
    __tablename__ = "autopool_deposit"

    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)

    shares: Mapped[float] = mapped_column(nullable=False)
    base_asset_amount: Mapped[float] = mapped_column(nullable=False)  # quantity of (WETH) or USDC or pxETH

    user: Mapped[str] = mapped_column(nullable=False)
    nav_per_share: Mapped[str] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(["autopool_vault_address", "chain_id"], ["autopools.vault_address", "autopools.chain_id"]),
    )


# extra
class AutopoolWithdrawal(Base):
    __tablename__ = "autopool_withdrawal"

    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)

    shares: Mapped[float] = mapped_column(nullable=False)
    base_asset_amount: Mapped[float] = mapped_column(nullable=False)  # quantity of (WETH) or USDC or pxETH

    user: Mapped[str] = mapped_column(nullable=False)
    nav_per_share: Mapped[float] = mapped_column(nullable=False)  # on deposit

    actualized_nav_per_share: Mapped[float] = mapped_column(
        nullable=False
    )  # the actual ratio of base asset amount / shares they got out
    slippage: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(["autopool_vault_address", "chain_id"], ["autopools.vault_address", "autopools.chain_id"]),
    )


# extra
class AutopoolFees(Base):
    __tablename__ = "autopool_fees"
    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    event_name: Mapped[str] = mapped_column(primary_key=True)

    denominated_in: Mapped[str] = mapped_column(nullable=False)
    minted_shares: Mapped[float] = mapped_column(nullable=False)
    minted_shares_value: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(["autopool_vault_address", "chain_id"], ["autopools.vault_address", "autopools.chain_id"]),
    )


# depends on Destination Token States for safe and spot values
# needed
class DestinationStates(Base):
    __tablename__ = "destination_states"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    # information about the destination itself at this moment in time

    incentive_apr: Mapped[float] = mapped_column(nullable=False)
    fee_and_base_apr: Mapped[float] = mapped_column(nullable=False)
    points_apr: Mapped[float] = mapped_column(nullable=True)

    total_apr_in: Mapped[float] = mapped_column(
        nullable=True
    )  # get destination summaryStats (in, and out) are seperate calls
    total_apr_out: Mapped[float] = mapped_column(nullable=True)

    # underlying_token_total_staked: Mapped[float] = mapped_column(nullable=True) # pretty sure I don't need this
    underlying_token_total_supply: Mapped[float] = mapped_column(nullable=False)
    safe_total_supply: Mapped[float] = mapped_column(nullable=True)  # only for pre autoUSD destinations

    # this is as lp tokens # via
    underlying_safe_price: Mapped[float] = mapped_column(nullable=False)
    underlying_spot_price: Mapped[float] = mapped_column(nullable=False)
    underlying_backing: Mapped[float] = mapped_column(nullable=False)
    denominated_in: Mapped[str] = mapped_column(nullable=False)  # should live in the destination

    safe_backing_discount: Mapped[float] = mapped_column(nullable=True)
    safe_spot_spread: Mapped[float] = mapped_column(nullable=True)
    spot_backing_discount: Mapped[float] = mapped_column(nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(
            ["destination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
    )


# depends on DestinationTokenState for value,
# just need destination total supply here as well
# needed
class AutopoolDestinationStates(Base):
    # information about this one autopool's lp tokens at this destination
    __tablename__ = "autopool_destination_states"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    # maybe rename to "balance_of?" not certain
    amount: Mapped[float] = mapped_column(nullable=False)  # how many lp tokens this autopool has here, lens contract

    # all
    total_safe_value: Mapped[float] = mapped_column(
        nullable=False
    )  # given the value of the lp tokens in the pool how much value does the atuopool have here
    total_spot_value: Mapped[float] = mapped_column(nullable=False)
    total_backing_value: Mapped[float] = mapped_column(nullable=False)

    percent_ownership: Mapped[float] = mapped_column(
        nullable=False
    )  # 100  * amount / destination_states.underlying_token_total_supply

    __table_args__ = (
        ForeignKeyConstraint(
            ["destination_vault_address", "block", "chain_id"],
            ["destination_states.destination_vault_address", "destination_states.block", "destination_states.chain_id"],
        ),
        ForeignKeyConstraint(["autopool_vault_address", "chain_id"], ["autopools.vault_address", "autopools.chain_id"]),
    )


# extra
class DebtReporting(Base):
    __tablename__ = "debt_reporting"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), nullable=False)

    denominated_in: Mapped[str] = mapped_column(nullable=False)
    base_asset_value: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["destination_vault_address", "block", "chain_id"],
            [
                "destination_states.destination_vault_address",
                "destination_states.block",
                "destination_states.chain_id",
            ],
        ),
        ForeignKeyConstraint(["autopool_vault_address", "chain_id"], ["autopools.vault_address", "autopools.chain_id"]),
    )


# extra
class ChainlinkGasCosts(Base):
    __tablename__ = "chainlink_gas_costs"

    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    chainlink_topic_id: Mapped[int] = mapped_column(nullable=False)
    gas_cost_in_eth_with_chainlink_premium: Mapped[float] = mapped_column(nullable=False)


# needed
class RebalancePlan(Base):
    __tablename__ = "rebalance_plan"

    file_name: Mapped[str] = mapped_column(nullable=False, primary_key=True)

    datetime_generated: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)
    autopool: Mapped[str] = mapped_column(nullable=False)
    chain_id: Mapped[int] = mapped_column(nullable=False)

    dex_aggregator: Mapped[str] = mapped_column(nullable=False)

    solver_address: Mapped[str] = mapped_column(nullable=False)
    rebalance_type: Mapped[str] = mapped_column(nullable=False)

    # sometimes this has different destinations but the same underlying token. that means
    destination_out: Mapped[str] = mapped_column(nullable=False)
    token_out: Mapped[str] = mapped_column(nullable=False)

    destination_in: Mapped[str] = mapped_column(nullable=False)
    token_in: Mapped[str] = mapped_column(nullable=False)

    move_name: Mapped[str] = mapped_column(nullable=False)  # f"{data['destinationOut']} -> {data['destinationIn']}"

    amount_out: Mapped[float] = mapped_column(nullable=False)

    # verify if this is safe or spot values
    amount_out_safe_value: Mapped[float] = mapped_column(nullable=False)
    min_amount_in_safe_value: Mapped[float] = mapped_column(nullable=False)
    min_amount_in: Mapped[float] = mapped_column(nullable=False)

    out_spot_eth: Mapped[float] = mapped_column(nullable=False)
    out_dest_apr: Mapped[float] = mapped_column(nullable=False)

    in_dest_apr: Mapped[float] = mapped_column(nullable=False)
    int_spot_eth: Mapped[float] = mapped_column(nullable=False)
    in_dest_adj_apr: Mapped[float] = mapped_column(nullable=False)

    apr_delta: Mapped[float] = mapped_column(nullable=False)
    swap_offset_period: Mapped[int] = mapped_column(nullable=False)

    candidate_destinations: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)
    candidate_destinations_rank: Mapped[int] = mapped_column(nullable=False)

    projected_swap_cost: Mapped[float] = mapped_column(nullable=False)
    projected_slippage: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["destination_in", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
        ForeignKeyConstraint(
            ["destination_out", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
        ForeignKeyConstraint(["token_in", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["token_out", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["autopool", "chain_id"], ["autopools.vault_address", "autopools.chain_id"]),
    )

    # dex steps here?


# needed
class RebalanceEvents(Base):
    __tablename__ = "rebalance_events"

    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    rebalance_file_path: Mapped[str] = mapped_column(ForeignKey("rebalance_plan.file_name"))

    quanity_out: Mapped[float] = mapped_column(nullable=False)
    safe_value_out: Mapped[float] = mapped_column(nullable=False)
    spot_value_out: Mapped[float] = mapped_column(nullable=False)
    backing_value_out: Mapped[float] = mapped_column(nullable=False)  # not used but can be useful later

    quanity_in: Mapped[float] = mapped_column(nullable=False)
    safe_value_in: Mapped[float] = mapped_column(nullable=False)
    spot_value_in: Mapped[float] = mapped_column(nullable=False)
    backing_value_in: Mapped[float] = mapped_column(nullable=False)  # not used but can be useful later

    actual_swap_cost: Mapped[float] = mapped_column(nullable=False)
    predicted_gain: Mapped[float] = mapped_column(nullable=False)
    break_even_days: Mapped[float] = mapped_column(nullable=False)
    actual_slippage: Mapped[float] = mapped_column(nullable=False)

    predicted_gain_during_swap_cost_off_set_period: Mapped[float] = mapped_column(nullable=False)
    predicted_increase_after_swap_cost: Mapped[float] = mapped_column(nullable=False)


# extra
class SolverProfit(Base):
    __tablename__ = "solver_profit"

    tx_hash: Mapped[str] = mapped_column(ForeignKey("rebalance_events.tx_hash"), primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    denominated_in: Mapped[str] = mapped_column(nullable=False)

    solver_value_held_before_rebalance: Mapped[float] = mapped_column(nullable=False)
    solver_value_held_after_rebalance: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


# done
class TokenValues(Base):
    __tablename__ = "token_values"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    token_address: Mapped[str] = mapped_column(primary_key=True)

    denomiated_in: Mapped[str] = mapped_column(nullable=False)
    backing: Mapped[float] = mapped_column(nullable=True)
    safe_price: Mapped[float] = mapped_column(nullable=True)
    safe_backing_spread: Mapped[float] = mapped_column(nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
    )


# done
class DestinationTokenValues(Base):
    __tablename__ = "destination_token_values"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    token_address: Mapped[str] = mapped_column(primary_key=True)
    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)

    spot_price: Mapped[float] = mapped_column(nullable=True)
    quantity: Mapped[float] = mapped_column(nullable=True)  # how many of this asset is in this pool.
    safe_spot_spread: Mapped[float] = mapped_column(nullable=True)
    spot_backing_discount: Mapped[float] = mapped_column(nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(
            ["destination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
    )


# extra
class IncentiveTokenLiquidations(Base):
    __tablename__ = "incentive_token_liquidations"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    token_address: Mapped[str] = mapped_column(primary_key=True)
    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)  # what destination this token is sold for

    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), nullable=False)

    acheived_price: Mapped[float] = mapped_column(nullable=False)
    safe_price: Mapped[float] = mapped_column(nullable=True)  # points to tokens values
    incentive_calculator_price: Mapped[float] = mapped_column(nullable=False)

    buy_amount: Mapped[float] = mapped_column(nullable=False)
    sell_amount: Mapped[float] = mapped_column(nullable=False)

    # can get what this is denomicated in form looking up the table,
    denominated_in: Mapped[str] = mapped_column(nullable=False)  # USDC, WETH

    incentive_calculator_price_diff_with_acheived: Mapped[float] = mapped_column(nullable=False)
    safe_price_diff_with_acheived: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(
            ["destination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
    )


# # I think this is an anti pattern, I don't want to keep it
# class LastAutopoolUpdated(Base):
#     __tablename__ = "last_autopool_updated"

#     table_name: Mapped[str] = mapped_column(primary_key=True)
#     block: Mapped[int] = mapped_column(nullable=False)
#     autopool: Mapped[str] = mapped_column(nullable=False)


# class LastChainUpdated(Base):
#     __tablename__ = "last_chain_updated"

#     table_name: Mapped[str] = mapped_column(primary_key=True)
#     block: Mapped[int] = mapped_column(nullable=False)
#     chain_id: Mapped[str] = mapped_column(nullable=False)


def drop_and_full_rebuild_db():
    meta = MetaData()
    meta.reflect(bind=ENGINE)
    meta.drop_all(bind=ENGINE)
    print("Dropped all existing tables.")
    Base.metadata.create_all(bind=ENGINE)
    print("Recreated all tables from ORM definitions.")


Session = sessionmaker(bind=ENGINE)

if __name__ == "__main__":
    drop_and_full_rebuild_db()
