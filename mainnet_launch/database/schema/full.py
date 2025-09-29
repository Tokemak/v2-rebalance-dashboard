from dataclasses import asdict
from dotenv import load_dotenv
from urllib.parse import urlparse
import os
import pandas as pd
import pydot

from typing import Optional

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column
from sqlalchemy import DateTime, ForeignKey, ARRAY, String, ForeignKeyConstraint, BigInteger, Integer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import UUID, ARRAY
import uuid

load_dotenv()

tmpPostgres = urlparse(os.getenv("ADD_PLASMA_MAIN_DATABASE_FORK"))


# which_database = os.getenv("WHICH_DATABASE")
# if which_database is None:
#     raise ValueError("WHICH_DATABASE environment variable not set")
# elif which_database == "MAIN_DATABASE_URL":
#     tmpPostgres = urlparse(os.getenv("MAIN_DATABASE_URL"))
# elif which_database == "MAIN_READ_REPLICA_DATABASE_URL":
#     tmpPostgres = urlparse(os.getenv("MAIN_READ_REPLICA_DATABASE_URL"))
# else:
#     raise ValueError(f"WHICH_DATABASE environment variable set to invalid value: {which_database}")


ENGINE = create_engine(
    f"postgresql+psycopg2://{tmpPostgres.username}:{tmpPostgres.password}"
    f"@{tmpPostgres.hostname}{tmpPostgres.path}?sslmode=require",
    echo=False,  # Enable SQL query logging for debugging.
    pool_pre_ping=True,  # ← test connections before using them
    pool_timeout=30,  # wait for a free conn before error
    pool_size=5,  # keep 5 open connections
    max_overflow=0,  # don’t spin up “extra” ones
)


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
    effective_gas_price: Mapped[int] = mapped_column(BigInteger, nullable=False)  # pretty sure this is just gas price
    gas_used: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # this is gas_used * effective_gas_price # is redundent
    gas_cost_in_eth: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class Tokens(Base):
    __tablename__ = "tokens"

    token_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    symbol: Mapped[str] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    decimals: Mapped[int] = mapped_column(nullable=False)


class Autopools(Base):
    __tablename__ = "autopools"
    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    block_deployed: Mapped[int] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)
    strategy_address: Mapped[str] = mapped_column(nullable=True)
    base_asset: Mapped[str] = mapped_column(nullable=False)
    data_from_rebalance_plan: Mapped[bool] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block_deployed", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class Destinations(Base):
    __tablename__ = "destinations"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    exchange_name: Mapped[str] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)
    pool_type: Mapped[str] = mapped_column(nullable=False)
    pool: Mapped[str] = mapped_column(nullable=False)
    underlying: Mapped[str] = mapped_column(nullable=False)
    underlying_symbol: Mapped[str] = mapped_column(nullable=False)
    underlying_name: Mapped[str] = mapped_column(nullable=False)

    denominated_in: Mapped[str] = mapped_column(nullable=False)  # DestinationVaultAddress.baseAsset()
    destination_vault_decimals: Mapped[int] = mapped_column(nullable=False)  # DestinationVaultAddress.decimals()


class AutopoolDestinations(Base):
    __tablename__ = "autopool_destinations"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["destination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"],
            ["autopools.autopool_vault_address", "autopools.chain_id"],
        ),
    )


class DestinationTokens(Base):
    __tablename__ = "destination_tokens"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    token_address: Mapped[str] = mapped_column(primary_key=True)

    index: Mapped[int] = mapped_column(nullable=False)  # the order of this token in the destination tokens

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


class AutopoolStates(Base):
    __tablename__ = "autopool_states"

    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)

    total_shares: Mapped[float] = mapped_column(nullable=True)
    total_nav: Mapped[float] = mapped_column(nullable=True)
    nav_per_share: Mapped[float] = mapped_column(
        nullable=True
    )  # not 1:1 with total_shares and total_nav uses convertToAssets(1e18)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
    )


class DestinationStates(Base):
    __tablename__ = "destination_states"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    incentive_apr: Mapped[float] = mapped_column(nullable=True)
    fee_apr: Mapped[float] = mapped_column(nullable=True)
    base_apr: Mapped[float] = mapped_column(nullable=True)
    points_apr: Mapped[float] = mapped_column(nullable=True)
    # only for post autoUSD destinations
    fee_plus_base_apr: Mapped[float] = mapped_column(nullable=True)
    total_apr_in: Mapped[float] = mapped_column(nullable=True)
    total_apr_out: Mapped[float] = mapped_column(nullable=True)
    underlying_token_total_supply: Mapped[float] = mapped_column(nullable=True)
    safe_total_supply: Mapped[float] = mapped_column(nullable=True)
    lp_token_spot_price: Mapped[float] = mapped_column(nullable=True)
    lp_token_safe_price: Mapped[float] = mapped_column(nullable=True)
    from_rebalance_plan: Mapped[bool] = mapped_column(nullable=False)
    rebalance_plan_timestamp: Mapped[int] = mapped_column(nullable=True)
    rebalance_plan_key: Mapped[str] = mapped_column(nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(
            ["destination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
    )


class AutopoolDestinationStates(Base):
    __tablename__ = "autopool_destination_states"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    owned_shares: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["destination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
    )


class TokenValues(Base):
    __tablename__ = "token_values"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    token_address: Mapped[str] = mapped_column(primary_key=True)
    denominated_in: Mapped[str] = mapped_column(primary_key=True)

    backing: Mapped[float] = mapped_column(nullable=True)
    safe_price: Mapped[float] = mapped_column(nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
    )


# largest table, can make smaller
class DestinationTokenValues(Base):
    __tablename__ = "destination_token_values"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)  # can make smaller?, smaller dtype?
    token_address: Mapped[str] = mapped_column(primary_key=True)
    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    denominated_in: Mapped[str] = mapped_column(
        primary_key=True
    )  # we don't need this, it the same as destinations.base_asset()

    spot_price: Mapped[float] = mapped_column(nullable=True)
    quantity: Mapped[float] = mapped_column(nullable=True)  # scaled by token decimals

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(
            ["destination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
    )


# needed
class RebalancePlans(Base):
    __tablename__ = "rebalance_plans"

    file_name: Mapped[str] = mapped_column(primary_key=True)

    datetime_generated: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)
    autopool_vault_address: Mapped[str] = mapped_column(nullable=False)
    chain_id: Mapped[int] = mapped_column(nullable=False)
    solver_address: Mapped[str] = mapped_column(nullable=True)
    rebalance_type: Mapped[str] = mapped_column(nullable=True)
    destination_out: Mapped[str] = mapped_column(nullable=True)
    token_out: Mapped[str] = mapped_column(nullable=True)
    destination_in: Mapped[str] = mapped_column(nullable=True)
    token_in: Mapped[str] = mapped_column(nullable=True)

    # TODO consider removing move name, it is inferable from the destination names
    move_name: Mapped[str] = mapped_column(nullable=True)

    amount_out: Mapped[float] = mapped_column(nullable=True)
    amount_out_safe_value: Mapped[float] = mapped_column(nullable=True)

    min_amount_in: Mapped[float] = mapped_column(nullable=True)
    min_amount_in_safe_value: Mapped[float] = mapped_column(nullable=True)

    amount_out_spot_value: Mapped[float] = mapped_column(nullable=True)
    out_dest_apr: Mapped[float] = mapped_column(nullable=True)

    min_amount_in_spot_value: Mapped[float] = mapped_column(nullable=True)
    in_dest_apr: Mapped[float] = mapped_column(nullable=True)
    in_dest_adj_apr: Mapped[float] = mapped_column(nullable=True)

    apr_delta: Mapped[float] = mapped_column(nullable=True)
    swap_offset_period: Mapped[int] = mapped_column(nullable=True)

    num_candidate_destinations: Mapped[int] = mapped_column(nullable=True)
    candidate_destinations_rank: Mapped[int] = mapped_column(nullable=True)

    projected_swap_cost: Mapped[float] = mapped_column(nullable=True)
    projected_net_gain: Mapped[float] = mapped_column(nullable=True)
    projected_gross_gain: Mapped[float] = mapped_column(nullable=True)

    projected_slippage: Mapped[float] = mapped_column(nullable=True)  # 100 projected_swap_cost / out_spot_eth

    __table_args__ = (
        ForeignKeyConstraint(
            ["destination_in", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
        ForeignKeyConstraint(
            ["destination_out", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
    )


class DexSwapSteps(Base):
    __tablename__ = "dex_swap_steps"

    file_name: Mapped[str] = mapped_column(primary_key=True)
    step_index: Mapped[int] = mapped_column(primary_key=True)
    dex: Mapped[str] = mapped_column(nullable=False)
    aggregator_names: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=True)

    # maybe add how much value is moved in this step
    # consider adding the other step data here as needed
    # consider adding info about the route here too
    __table_args__ = (ForeignKeyConstraint(["file_name"], ["rebalance_plans.file_name"]),)


# extra
class RebalanceCandidateDestinations(Base):
    __tablename__ = "rebalance_candidate_destinations"

    file_name: Mapped[str] = mapped_column(primary_key=True)

    desination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(nullable=False)
    net_gain: Mapped[float] = mapped_column(nullable=False)
    expected_swap_cost: Mapped[float] = mapped_column(nullable=False)
    gross_gain_during_swap_cost_offset_period: Mapped[float | None] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["desination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
        ForeignKeyConstraint(["file_name"], ["rebalance_plans.file_name"]),
    )


class RebalanceEvents(Base):
    __tablename__ = "rebalance_events"
    tx_hash: Mapped[str] = mapped_column(primary_key=True)

    autopool_vault_address: Mapped[str] = mapped_column(nullable=False)
    chain_id: Mapped[int] = mapped_column(nullable=False)
    rebalance_file_path: Mapped[str] = mapped_column(nullable=True)  # TODO, this is not nullable
    destination_out: Mapped[str] = mapped_column(nullable=False)
    destination_in: Mapped[str] = mapped_column(nullable=False)

    quantity_out: Mapped[float] = mapped_column(
        nullable=False
    )  # don't trust these, not correct, use UnderlyingWithdraw and UnderlyingDeposit
    quantity_in: Mapped[float] = mapped_column(nullable=False)

    safe_value_out: Mapped[float] = mapped_column(nullable=False)
    safe_value_in: Mapped[float] = mapped_column(nullable=False)

    spot_value_in: Mapped[float] = mapped_column(nullable=False)
    spot_value_out: Mapped[float] = mapped_column(nullable=False)

    spot_value_in_solver_change: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"],
            ["autopools.autopool_vault_address", "autopools.chain_id"],
        ),
        ForeignKeyConstraint(
            ["destination_out", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
        ForeignKeyConstraint(
            ["destination_in", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
        ForeignKeyConstraint(["rebalance_file_path"], ["rebalance_plans.file_name"]),
        ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),
    )


# not populated
class SolverProfit(Base):
    __tablename__ = "solver_profit"

    tx_hash: Mapped[str] = mapped_column(ForeignKey("rebalance_events.tx_hash"), primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    denominated_in: Mapped[str] = mapped_column(nullable=False)

    solver_value_held_before_rebalance: Mapped[float] = mapped_column(nullable=False)
    solver_value_held_after_rebalance: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class IncentiveTokenSwapped(Base):
    __tablename__ = "incentive_token_swapped"

    tx_hash: Mapped[str] = mapped_column(primary_key=True)
    log_index: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(
        nullable=False
    )  # technically redundent, but we need it for foreign keys to tokens
    liquidation_row: Mapped[str] = mapped_column(nullable=False)

    sell_token_address: Mapped[str] = mapped_column(nullable=False)
    buy_token_address: Mapped[str] = mapped_column(nullable=False)

    # normalized, scaled by decimals
    sell_amount: Mapped[float] = mapped_column(nullable=False)
    buy_amount: Mapped[float] = mapped_column(nullable=False)  # how much we expected
    buy_amount_received: Mapped[float] = mapped_column(nullable=False)  # how much we actually got

    __table_args__ = (
        ForeignKeyConstraint(["sell_token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["buy_token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),
    )


class IncentiveTokenPrices(Base):
    __tablename__ = "incentive_token_sale_prices"

    tx_hash: Mapped[str] = mapped_column(primary_key=True)
    log_index: Mapped[int] = mapped_column(primary_key=True)
    third_party_price: Mapped[float] = mapped_column(nullable=True)
    # the price according to our internal historical prices api

    __table_args__ = (
        ForeignKeyConstraint(
            ["tx_hash", "log_index"], ["incentive_token_swapped.tx_hash", "incentive_token_swapped.log_index"]
        ),
    )


class ChainlinkGasCosts(Base):
    __tablename__ = "chainlink_gas_costs"

    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    chainlink_topic_id: Mapped[str] = mapped_column(nullable=False)


class AutopoolFees(Base):
    __tablename__ = "autopool_fees"
    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    log_index: Mapped[int] = mapped_column(primary_key=True)

    autopool_vault_address: Mapped[str] = mapped_column(nullable=False)
    fee_name: Mapped[str] = mapped_column(nullable=False)  # eg FeeCollected or PeriodicFeeCollected

    fee_sink: Mapped[str] = mapped_column(nullable=False)  # where the fee went
    minted_shares: Mapped[float] = mapped_column(nullable=False)  # shares is always in 1e18


# not populated
class AutopoolWithdrawalToken(Base):
    # for when the user can withdraw the LP tokens, not the base asset
    __tablename__ = "autopool_withdrawal_token"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"))
    token_address: Mapped[str] = mapped_column(nullable=False)
    amount: Mapped[float] = mapped_column(nullable=False)


class AutopoolDeposit(Base):
    __tablename__ = "autopool_deposits"

    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    tx_hash: Mapped[str] = mapped_column(primary_key=True)
    log_index: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    shares: Mapped[float] = mapped_column(nullable=False)
    assets: Mapped[float] = mapped_column(nullable=False)

    sender: Mapped[str] = mapped_column(nullable=False)
    owner: Mapped[str] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
        ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),
    )


class AutopoolWithdrawal(Base):
    __tablename__ = "autopool_withdrawals"
    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    tx_hash: Mapped[str] = mapped_column(primary_key=True)
    log_index: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    shares: Mapped[float] = mapped_column(nullable=False)
    assets: Mapped[float] = mapped_column(nullable=False)

    sender: Mapped[str] = mapped_column(nullable=False)
    receiver: Mapped[str] = mapped_column(nullable=False)
    owner: Mapped[str] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
        ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),
    )


class AutopoolTransfer(Base):
    __tablename__ = "autopool_transfers"
    # ERC20.Transfer events for Autopool shares moved between accounts

    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    log_index: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(nullable=False)

    autopool_vault_address: Mapped[str] = mapped_column(nullable=False)
    from_address: Mapped[str] = mapped_column(nullable=False)
    to_address: Mapped[str] = mapped_column(nullable=False)
    value: Mapped[float] = mapped_column(nullable=False)  # always in 1e18


# not populated
class DexScreenerPoolLiquidity(Base):
    __tablename__ = "dex_screener_pool_liquidity"

    pool_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    pool_name: Mapped[str] = mapped_column(nullable=False)
    pool_symbol: Mapped[str] = mapped_column(nullable=False)
    dex: Mapped[str] = mapped_column(nullable=False)  # curve, balancer, uniswap, etc

    # sorted, order of tokens in the pool
    pool_tokens: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)


# not populated
class PoolLiquiditySnapshot(Base):
    __tablename__ = "pool_liquidity_snapshot"
    # according to dex screener how but USD tvl is in it at this point in time

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    chain_id: Mapped[int] = mapped_column(nullable=False)
    pool_address: Mapped[str] = mapped_column(nullable=False)
    token_address: Mapped[str] = mapped_column(nullable=False)
    usd_liquidity: Mapped[float] = mapped_column(nullable=False)
    datetime_requested: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)
    datetime_received: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["pool_address", "chain_id"],
            ["dex_screener_pool_liquidity.pool_address", "dex_screener_pool_liquidity.chain_id"],
        ),
        ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
    )


class SwapQuote(Base):
    __tablename__ = "swap_quotes"

    chain_id: Mapped[int] = mapped_column(nullable=False)
    base_asset: Mapped[str] = mapped_column(nullable=False)  # eg WETH, USDC, DOLA
    api_name: Mapped[str] = mapped_column(nullable=False)  # eg tokemak, or odos (so far)

    sell_token_address: Mapped[str] = mapped_column(nullable=False)
    buy_token_address: Mapped[str] = mapped_column(nullable=False)

    scaled_amount_in: Mapped[float] = mapped_column(nullable=False)
    scaled_amount_out: Mapped[float] = mapped_column(nullable=False)

    pools_blacklist: Mapped[str] = mapped_column(nullable=True)
    # how big a pool needs to be before it is excluded, only used by odos
    percent_exclude_threshold: Mapped[float] = mapped_column(nullable=False)

    aggregator_name: Mapped[str] = mapped_column(nullable=False)  # the aggregator name used for this swap
    datetime_received: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)

    quote_batch: Mapped[int] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(["sell_token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["buy_token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
    )

    # must be the last column in the table, not certain why
    # Client-generated PK; never pass it in manually
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default_factory=uuid.uuid4,  # <-- ensures Python value exists before to_tuple()
    )


class AssetExposure(Base):
    __tablename__ = "asset_exposure"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    reference_asset: Mapped[str] = mapped_column(primary_key=True)  # eg WETH, USDC, DOLA
    token_address: Mapped[str] = mapped_column(primary_key=True)

    quantity: Mapped[float] = mapped_column(nullable=False)  # in scaled terms, (eg 1 for ETH instead of 1e18)

    quote_batch: Mapped[int] = mapped_column(nullable=False)  # eg what run this quote used to group quotes

    __table_args__ = (
        ForeignKeyConstraint(["reference_asset", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
    )


class DestinationUnderlyingDeposited(Base):
    __tablename__ = "destination_underlying_deposited"
    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), nullable=False, primary_key=True)

    destination_vault_address: Mapped[str] = mapped_column(nullable=False, primary_key=True)

    amount: Mapped[str] = mapped_column(nullable=False)  # unscaled quantity of lp tokens (or recipt tokens for lending)
    sender: Mapped[str] = mapped_column(nullable=False)  # the autopool_vault_address, I'm pretty sure

    __table_args__ = (ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),)


class DestinationUnderlyingWithdraw(Base):
    __tablename__ = "destination_underlying_withdraw"
    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), nullable=False, primary_key=True)

    destination_vault_address: Mapped[str] = mapped_column(nullable=False, primary_key=True)
    # unscaled quantity of lp tokens (or receipt tokens for lending)
    amount: Mapped[str] = mapped_column(nullable=False)
    owner: Mapped[str] = mapped_column(nullable=False)  # the autopool_vault_address, I'm pretty sure
    to_address: Mapped[str] = mapped_column(nullable=False)


def drop_and_full_rebuild_db():
    confirmation = input("Type 'delete_and_rebuild' to confirm dropping and rebuilding the database: ")
    if confirmation != "delete_and_rebuild":
        print("Operation canceled. The database was not modified.")
        return

    meta = MetaData()
    meta.reflect(bind=ENGINE)
    meta.drop_all(bind=ENGINE)
    print("Dropped all existing tables.")
    Base.metadata.create_all(bind=ENGINE)
    print("Recreated all tables from ORM definitions.")


def reflect_and_create():
    print("reflecting and creating Schema")
    meta = MetaData()
    meta.reflect(bind=ENGINE)
    Base.metadata.create_all(bind=ENGINE)


Session = sessionmaker(bind=ENGINE)


if __name__ == "__main__":
    reflect_and_create()
    # drop_and_full_rebuild_db()

    pass
