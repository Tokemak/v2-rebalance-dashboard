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

# tmpPostgres = urlparse(os.getenv("MAIN_DATABASE_URL"))
#  DEV_LOCAL_DATABASE_URL
# tmpPostgres = urlparse(os.getenv("DEV_LOCAL_DATABASE_URL"))
tmpPostgres = urlparse(os.getenv("ADD_SONIC_DATABASE_URL"))


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
    gas_cost_in_eth: Mapped[float] = mapped_column(nullable=False)  # gas_used * effective_gas_price

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


# done
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
    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    block_deployed: Mapped[int] = mapped_column(nullable=False)  # not certain I care about this

    name: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)

    # not certain if the strategy address can be changed
    strategy_address: Mapped[str] = mapped_column(nullable=True)

    base_asset: Mapped[str] = mapped_column(nullable=False)

    data_from_rebalance_plan: Mapped[bool] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block_deployed", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


# done
class Destinations(Base):
    __tablename__ = "destinations"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    exchange_name: Mapped[str] = mapped_column(nullable=False)
    # block_deployed: Mapped[int] = mapped_column(nullable=False)

    name: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)  # not certain here on if we should have both names and symbols

    pool_type: Mapped[str] = mapped_column(nullable=False)
    pool: Mapped[str] = mapped_column(nullable=False)
    underlying: Mapped[str] = mapped_column(nullable=False)
    underlying_symbol: Mapped[str] = mapped_column(nullable=False)
    underlying_name: Mapped[str] = mapped_column(nullable=False)

    denominated_in: Mapped[str] = mapped_column(nullable=False)  # DestinationVaultAddress.baseAsset()
    destination_vault_decimals: Mapped[int] = mapped_column(nullable=False)  # DestinationVaultAddress.decimals()

    # maybe add block deployed, timestamp deployed?


class AutopoolDestinations(Base):

    __tablename__ = "autopool_destinations"

    # all ever autopool destinations from destinationVaultAdded events
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


# done
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


# done
class AutopoolStates(Base):
    __tablename__ = "autopool_states"

    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)

    # these are view on the autopool_vault_address
    total_shares: Mapped[float] = mapped_column(nullable=True)
    total_nav: Mapped[float] = mapped_column(nullable=True)
    nav_per_share: Mapped[float] = mapped_column(nullable=True)  # not 1:1, uses convert to assets(1e18)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
    )


# done
class DestinationStates(Base):
    __tablename__ = "destination_states"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    incentive_apr: Mapped[float] = mapped_column(nullable=True)
    fee_apr: Mapped[float] = mapped_column(nullable=True)
    base_apr: Mapped[float] = mapped_column(nullable=True)
    points_apr: Mapped[float] = mapped_column(nullable=True)
    # price_return: Mapped[float] = mapped_column(nullable=True)

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


# this is not getting balETH, autoLRT, and dineroETH unsure why
class AutopoolDestinationStates(Base):
    # information about this one autopool's lp tokens at this destination
    __tablename__ = "autopool_destination_states"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    # how many lp tokens this autopool has here, lens contract

    owned_shares: Mapped[float] = mapped_column(nullable=False)

    # this can be infered by the destinations states and token values
    # given the value of the lp tokens in the pool how much value does the atuopool have here
    # 0x40219bBda953ca811d2D0168Dc806a96b84791d9 and 0x40219bBda953ca811d2D0168Dc806a96b84791d9
    # and 0xc4Eb861e7b66f593482a3D7E8adc314f6eEDA30B
    # are not properly normalized,
    # total_safe_value: Mapped[float] = mapped_column(nullable=False)  # not correct
    # total_spot_value: Mapped[float] = mapped_column(nullable=False)
    # total_backing_value: Mapped[float] = mapped_column(nullable=False)

    # percent_ownership: Mapped[float] = mapped_column(
    #     nullable=False
    # )  # 100  * amount / destination_states.underlying_token_total_supply

    __table_args__ = (
        # ForeignKeyConstraint(
        #     ["destination_vault_address", "block", "chain_id"],
        #     ["destination_states.destination_vault_address", "destination_states.block", "destination_states.chain_id"],
        # ),
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
    )


# done
class TokenValues(Base):
    __tablename__ = "token_values"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    token_address: Mapped[str] = mapped_column(primary_key=True)

    denominated_in: Mapped[str] = mapped_column(primary_key=True)
    backing: Mapped[float] = mapped_column(nullable=True)
    safe_price: Mapped[float] = mapped_column(nullable=True)
    # safe_backing_discount: Mapped[float] = mapped_column(nullable=True) # inferable from  (safe_price - backing) / backing

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
    )


class DestinationTokenValues(Base):
    __tablename__ = "destination_token_values"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    token_address: Mapped[str] = mapped_column(primary_key=True)
    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    denominated_in: Mapped[str] = mapped_column(primary_key=True)

    # the spot price of this token in this destination, using our price oracle
    spot_price: Mapped[float] = mapped_column(nullable=True)

    # the quantity of this token in this pool at this point
    quantity: Mapped[float] = mapped_column(nullable=True)

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

    move_name: Mapped[str] = mapped_column(
        nullable=True
    )  # f"{data['destinationOut']} -> {data['destinationIn']}" ( I don't like this TODO pick a better move name)

    amount_out: Mapped[float] = mapped_column(nullable=True)
    # amountOutETH
    amount_out_safe_value: Mapped[float] = mapped_column(nullable=True)

    min_amount_in: Mapped[float] = mapped_column(nullable=True)
    # minAmountInETH
    min_amount_in_safe_value: Mapped[float] = mapped_column(nullable=True)

    amount_out_spot_value: Mapped[float] = mapped_column(nullable=True)
    out_dest_apr: Mapped[float] = mapped_column(nullable=True)

    min_amount_in_spot_value: Mapped[float] = mapped_column(nullable=True)
    in_dest_apr: Mapped[float] = mapped_column(nullable=True)
    in_dest_adj_apr: Mapped[float] = mapped_column(nullable=True)

    apr_delta: Mapped[float] = mapped_column(nullable=True)
    swap_offset_period: Mapped[int] = mapped_column(nullable=True)

    # not certain on if this should be a list or a second table,
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


# needed
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

    #     'Tokemak-Wrapped Ether-osETH/rETH', # destination name,
    #    8.017298132176219e+17,  (net gain during swap cost offset period),
    #    1.073271250808832e+17], expected swap cost
    # gross_gain_during_swap_cost_offset_period = 8.017298132176219e+17 + 1.073271250808832e+17

    __table_args__ = (
        ForeignKeyConstraint(
            ["desination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
        ForeignKeyConstraint(["file_name"], ["rebalance_plans.file_name"]),
    )


# needed
class RebalanceEvents(Base):
    __tablename__ = "rebalance_events"
    tx_hash: Mapped[str] = mapped_column(primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(nullable=False)
    chain_id: Mapped[int] = mapped_column(nullable=False)

    rebalance_file_path: Mapped[str] = mapped_column(nullable=True)

    destination_out: Mapped[str] = mapped_column(nullable=False)
    destination_in: Mapped[str] = mapped_column(nullable=False)

    quantity_out: Mapped[float] = mapped_column(nullable=False)
    quantity_in: Mapped[float] = mapped_column(nullable=False)

    safe_value_out: Mapped[float] = mapped_column(nullable=False)
    safe_value_in: Mapped[float] = mapped_column(nullable=False)

    spot_value_in: Mapped[float] = mapped_column(nullable=False)
    spot_value_out: Mapped[float] = mapped_column(nullable=False)

    spot_value_in_solver_change: Mapped[float] = mapped_column(nullable=False)

    # swap_offset_period: Mapped[int] = mapped_column(nullable=True) # in the rebalance plan
    # these are inferable
    # actual_swap_cost: Mapped[float] = mapped_column(nullable=False)
    # break_even_days: Mapped[float] = mapped_column(nullable=False)
    # actual_slippage: Mapped[float] = mapped_column(nullable=False)

    # predicted_gain_during_swap_cost_off_set_period: Mapped[float] = mapped_column(nullable=False)
    # predicted_increase_after_swap_cost: Mapped[float] = mapped_column(nullable=False)

    # make sure that you add

    # in order

    # rebalance_plans,
    # blocks,
    # destination_token_values (at this block) (connected from rebalance transactions)
    #

    # backing_value_out: Mapped[float] = mapped_column(nullable=False)  # not used but can be useful later

    # safe_value_in: Mapped[float] = mapped_column(nullable=False) # inferable later
    # spot_value_in: Mapped[float] = mapped_column(nullable=False)
    # backing_value_in: Mapped[float] = mapped_column(nullable=False)  # not used but can be useful later

    # actual_swap_cost: Mapped[float] = mapped_column(nullable=False)
    # break_even_days: Mapped[float] = mapped_column(nullable=False)
    # actual_slippage: Mapped[float] = mapped_column(nullable=False)

    # predicted_gain_during_swap_cost_off_set_period: Mapped[float] = mapped_column(nullable=False)
    # predicted_increase_after_swap_cost: Mapped[float] = mapped_column(nullable=False)

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


# extra
class IncentiveTokenLiquidations(Base):
    # not certain if this makes sense to belong to an autopool
    __tablename__ = "incentive_token_liquidations"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    token_address: Mapped[str] = mapped_column(primary_key=True)
    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)  # what destination this token is sold for

    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), nullable=False)

    achieved_price: Mapped[float] = mapped_column(nullable=False)
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


# extra
class DebtReporting(Base):
    # double check what this is used for
    __tablename__ = "debt_reporting"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), nullable=False)

    # denominated_in: Mapped[str] = mapped_column(nullable=False) # autopools.base_asset (can skip)
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
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
    )


class ChainlinkGasCosts(Base):
    __tablename__ = "chainlink_gas_costs"

    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    chainlink_topic_id: Mapped[str] = mapped_column(nullable=False)


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
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
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
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
    )


# extra
class AutopoolWithdrawalToken(Base):
    __tablename__ = "autopool_withdrawal_token"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"))
    # the toekn the user gets the token withdrawan in, eg WETH, LP tokens, etc
    token_address: Mapped[str] = mapped_column(nullable=False)
    # quantity of this token removed from the autopool
    amount: Mapped[float] = mapped_column(nullable=False)


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
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
    )


class DexScreenerPoolLiquidity(Base):
    __tablename__ = "dex_screener_pool_liquidity"
    # the pool it self, stateless

    pool_address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    pool_name: Mapped[str] = mapped_column(nullable=False)
    pool_symbol: Mapped[str] = mapped_column(nullable=False)
    dex: Mapped[str] = mapped_column(nullable=False)  # curve, balancer, uniswap, etc

    # sorted, order of tokens in the pool
    pool_tokens: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)


class PoolLiquiditySnapshot(Base):
    __tablename__ = "pool_liquidity_snapshot"
    # according to dex screener how but USD tvl is in

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
    __tablename__ = "swap_quote"

    chain_id: Mapped[int] = mapped_column(nullable=False)
    base_asset: Mapped[str] = mapped_column(nullable=False)  # eg WETH, USDC, DOLA
    api_name: Mapped[str] = mapped_column(nullable=False)  # eg 1inch, paraswap, etc

    sell_token_address: Mapped[str] = mapped_column(nullable=False)
    buy_token_address: Mapped[str] = mapped_column(nullable=False)

    scaled_amount_in: Mapped[float] = mapped_column(nullable=False)
    scaled_amount_out: Mapped[float] = mapped_column(nullable=False)

    pools_blacklist: Mapped[str] = mapped_column(nullable=True)  # pools to not use in the swap
    percent_exclude_threshold: Mapped[float] = mapped_column(
        nullable=False
    )  # how big a pool needs to be before it is excluded

    aggregator_name: Mapped[str] = mapped_column(nullable=False)  # the aggregator name used for this swap
    datetime_received: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)

    quote_batch: Mapped[int] = mapped_column(nullable=False)  # eg what run this quote used to group quotes
    size_factor: Mapped[str] = mapped_column(nullable=False)  # eg PORTION, or abosolute

    __table_args__ = (
        ForeignKeyConstraint(["sell_token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["buy_token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
    )

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

    __table_args__ = (
        ForeignKeyConstraint(["reference_asset", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
    )


def drop_and_full_rebuild_db():
    meta = MetaData()
    meta.reflect(bind=ENGINE)
    meta.drop_all(bind=ENGINE)
    print("Dropped all existing tables.")
    Base.metadata.create_all(bind=ENGINE)
    print("Recreated all tables from ORM definitions.")


def reflect_and_create():
    meta = MetaData()
    meta.reflect(bind=ENGINE)
    Base.metadata.create_all(bind=ENGINE)


Session = sessionmaker(bind=ENGINE)

if __name__ == "__main__":
    reflect_and_create()
