from dataclasses import asdict
from dotenv import load_dotenv
from urllib.parse import urlparse
import os
import pandas as pd

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column
from sqlalchemy import DateTime, ARRAY, String, ForeignKeyConstraint, BigInteger, Integer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import UUID, ARRAY
import uuid
from hexbytes import HexBytes

from custom_db_types import EvmAddress, EvmTxHash, EvmTopic, Base

load_dotenv()


# which_database = os.getenv("WHICH_DATABASE")

# if which_database is None:
#     raise ValueError("WHICH_DATABASE environment variable not set")
# elif which_database == "MAIN_DATABASE_URL":
#     tmpPostgres = urlparse(os.getenv("MAIN_DATABASE_URL"))
# elif which_database == "MAIN_READ_REPLICA_DATABASE_URL":
#     tmpPostgres = urlparse(os.getenv("MAIN_READ_REPLICA_DATABASE_URL"))
# else:
#     raise ValueError(f"WHICH_DATABASE environment variable set to invalid value: {which_database}")


# tmpPostgres = urlparse(os.getenv("LOCAL_MAIN_FORK_DATABASE_URL"))

tmpPostgres = urlparse(os.getenv("FROM_ZERO_DATABASE_URL"))

ENGINE = create_engine(
    f"postgresql+psycopg2://{tmpPostgres.username}:{tmpPostgres.password}"
    f"@{tmpPostgres.hostname}{tmpPostgres.path}?sslmode=require",
    echo=False,  # Enable SQL query logging for debugging.
    pool_pre_ping=True,
    pool_timeout=30,
    pool_size=5,
    max_overflow=0,
)

# -----------------------
# Core EVM data
# -----------------------

class Blocks(Base):
    __tablename__ = "blocks"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    datetime: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)


class Transactions(Base):
    __tablename__ = "transactions"

    tx_hash: Mapped[HexBytes] = mapped_column(EvmTxHash, primary_key=True)
    block: Mapped[int] = mapped_column(nullable=False)
    chain_id: Mapped[int] = mapped_column(nullable=False)

    from_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    to_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    effective_gas_price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    gas_used: Mapped[int] = mapped_column(BigInteger, nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class Tokens(Base):
    __tablename__ = "tokens"

    token_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    symbol: Mapped[str] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    decimals: Mapped[int] = mapped_column(nullable=False)


# -----------------------
# Tokemak Specific Data
# -----------------------

class Autopools(Base):
    __tablename__ = "autopools"
    autopool_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    block_deployed: Mapped[int] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)
    strategy_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=True)
    base_asset: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    data_from_rebalance_plan: Mapped[bool] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block_deployed", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class Destinations(Base):
    __tablename__ = "destinations"

    destination_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    exchange_name: Mapped[str] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)
    pool_type: Mapped[str] = mapped_column(nullable=False)
    pool: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    underlying: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    underlying_symbol: Mapped[str] = mapped_column(nullable=False)
    underlying_name: Mapped[str] = mapped_column(nullable=False)

    denominated_in: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)  # DestinationVaultAddress.baseAsset()
    destination_vault_decimals: Mapped[int] = mapped_column(nullable=False)  # DestinationVaultAddress.decimals()


class AutopoolDestinations(Base):
    __tablename__ = "autopool_destinations"

    destination_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    autopool_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)

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

    destination_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    token_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)

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

    autopool_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
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

    destination_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
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

    destination_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    autopool_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
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
    token_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    denominated_in: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)

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
    token_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    destination_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    # denominated_in: Mapped[str] = mapped_column(
    #     primary_key=True
    # )  # we don't need this, it the same as destinations.base_asset()

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


class RebalancePlans(Base):
    __tablename__ = "rebalance_plans"

    file_name: Mapped[str] = mapped_column(primary_key=True)

    datetime_generated: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)
    autopool_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    chain_id: Mapped[int] = mapped_column(nullable=False)
    solver_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    rebalance_type: Mapped[str] = mapped_column(nullable=True)
    destination_out: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=True)
    token_out: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=True)
    destination_in: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=True)
    token_in: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=True)

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

    # might be redundant too double chekc
    projected_swap_cost: Mapped[float] = mapped_column(nullable=True)
    projected_net_gain: Mapped[float] = mapped_column(nullable=True)
    projected_gross_gain: Mapped[float] = mapped_column(nullable=True)

    # redundant, can be calculated from projected_swap_cost and out_spot_eth
    # projected_slippage: Mapped[float] = mapped_column(nullable=True)  # 100 projected_swap_cost / out_spot_eth

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

    destination_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    chain_id: Mapped[int] = mapped_column(nullable=False)
    net_gain: Mapped[float] = mapped_column(nullable=False)
    expected_swap_cost: Mapped[float] = mapped_column(nullable=False)
    gross_gain_during_swap_cost_offset_period: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["destination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
        ForeignKeyConstraint(["file_name"], ["rebalance_plans.file_name"]),
    )


class RebalanceEvents(Base):
    __tablename__ = "rebalance_events"
    tx_hash: Mapped[HexBytes] = mapped_column(EvmTxHash, primary_key=True)

    autopool_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    chain_id: Mapped[int] = mapped_column(nullable=False)
    rebalance_file_path: Mapped[str] = mapped_column(nullable=False)
    destination_out: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    destination_in: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)

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


class IncentiveTokenSwapped(Base):
    __tablename__ = "incentive_token_swapped"

    tx_hash: Mapped[HexBytes] = mapped_column(EvmTxHash, primary_key=True)
    log_index: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(nullable=False)
    liquidation_row: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)

    sell_token_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    buy_token_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)

    # normalized, scaled by decimals
    sell_amount: Mapped[float] = mapped_column(nullable=False)
    buy_amount: Mapped[float] = mapped_column(nullable=False)  # how much we expected
    buy_amount_received: Mapped[float] = mapped_column(nullable=False)  # how much we actually got

    __table_args__ = (
        ForeignKeyConstraint(["sell_token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["buy_token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),
    )


class IncentiveTokenBalanceUpdated(Base):
    """
    Liqudation Row Balance Updated events

    Tracks how much of each token is ready for liqudation in the liqudation row contract

    Eg when liquidatable tokens are moved to the liqudation row.

    """

    __tablename__ = "incentive_token_balance_updated"

    tx_hash: Mapped[HexBytes] = mapped_column(EvmTxHash, primary_key=True)
    log_index: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    liquidation_row: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    token_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    destination_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    new_balance: Mapped[float] = mapped_column(nullable=False)  # the balance after updating. eg the balance value

    __table_args__ = (
        ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(
            ["destination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
        ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),
    )


class IncentiveTokenPrices(Base):
    __tablename__ = "incentive_token_sale_prices"

    tx_hash: Mapped[HexBytes] = mapped_column(EvmTxHash, primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    log_index: Mapped[int] = mapped_column(primary_key=True)

    # what token this is a price for, eg the buy token, where the price is in the sell token
    token_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    # the buy token
    token_price_denomiated_in: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    # the price according to our internal historical prices api
    third_party_price: Mapped[float] = mapped_column(nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["tx_hash", "log_index"], ["incentive_token_swapped.tx_hash", "incentive_token_swapped.log_index"]
        ),
    )


class ChainlinkGasCosts(Base):
    __tablename__ = "chainlink_gas_costs"

    tx_hash: Mapped[HexBytes] = mapped_column(EvmTxHash, primary_key=True)
    chainlink_topic_id: Mapped[HexBytes] = mapped_column(EvmTopic, nullable=False)

    __table_args__ = (ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),)


class AutopoolFees(Base):
    __tablename__ = "autopool_fees"
    tx_hash: Mapped[HexBytes] = mapped_column(EvmTxHash, primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    log_index: Mapped[int] = mapped_column(primary_key=True)

    autopool_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    fee_name: Mapped[str] = mapped_column(nullable=False)  # eg FeeCollected or PeriodicFeeCollected

    fee_sink: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)  # where the fee went
    minted_shares: Mapped[float] = mapped_column(nullable=False)  # shares is always in 1e18

    __table_args__ = (
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
        ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),
    )


# not populated
class AutopoolWithdrawalToken(Base):
    # for when the user can withdraw the LP tokens, not the base asset
    __tablename__ = "autopool_withdrawal_token"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tx_hash: Mapped[HexBytes] = mapped_column(EvmTxHash, nullable=False)
    token_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    amount: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),)


class AutopoolDeposit(Base):
    __tablename__ = "autopool_deposits"

    autopool_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    tx_hash: Mapped[HexBytes] = mapped_column(EvmTxHash, primary_key=True)
    log_index: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    shares: Mapped[float] = mapped_column(nullable=False)
    assets: Mapped[float] = mapped_column(nullable=False)

    sender: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    owner: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)

    # NOTE:
    # owner, receiver, and sender are not certain to be EOAs
    # need to check the actual beneficiaries in the txs as needed

    __table_args__ = (
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
        ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),
    )


class AutopoolWithdrawal(Base):
    __tablename__ = "autopool_withdrawals"
    autopool_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    tx_hash: Mapped[HexBytes] = mapped_column(EvmTxHash, primary_key=True)
    log_index: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    shares: Mapped[float] = mapped_column(nullable=False)
    assets: Mapped[float] = mapped_column(nullable=False)

    sender: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    receiver: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    owner: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
        ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),
    )


class AutopoolTransfer(Base):
    __tablename__ = "autopool_transfers"
    # ERC20.Transfer events for Autopool shares moved between accounts

    tx_hash: Mapped[HexBytes] = mapped_column(EvmTxHash, primary_key=True)
    log_index: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(nullable=False)

    autopool_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    from_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    to_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    value: Mapped[float] = mapped_column(nullable=False)  # always in 1e18

    __table_args__ = (ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),)


# not populated
class DexScreenerPoolLiquidity(Base):
    __tablename__ = "dex_screener_pool_liquidity"

    pool_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
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
    pool_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    token_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
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
    base_asset: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)  # eg WETH, USDC, DOLA

    api_name: Mapped[str] = mapped_column(nullable=False)  # eg tokemak, or odos (so far)

    sell_token_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)
    buy_token_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)

    scaled_amount_in: Mapped[float] = mapped_column(nullable=False)
    scaled_amount_out: Mapped[float] = mapped_column(nullable=False)

    # content having this as a list of strings is easier to work with than a single strings since it is not used that much
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
    reference_asset: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)
    token_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)

    quantity: Mapped[float] = mapped_column(nullable=False)  # in scaled terms, (eg 1 for ETH instead of 1e18)

    quote_batch: Mapped[int] = mapped_column(
        nullable=False
    )  # helper for iding the (group of quotes) all used in the same time

    __table_args__ = (
        ForeignKeyConstraint(["reference_asset", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.token_address", "tokens.chain_id"]),
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
    )


class DestinationUnderlyingDeposited(Base):
    __tablename__ = "destination_underlying_deposited"
    tx_hash: Mapped[HexBytes] = mapped_column(EvmTxHash, primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    log_index: Mapped[int] = mapped_column(primary_key=True)
    destination_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)

    amount: Mapped[str] = mapped_column(nullable=False)  # unscaled quantity of tokens
    sender: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)  # the autopool_vault_address, I'm pretty sure

    __table_args__ = (
        ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),
        ForeignKeyConstraint(
            ["destination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
    )


class DestinationUnderlyingWithdraw(Base):
    __tablename__ = "destination_underlying_withdraw"
    tx_hash: Mapped[HexBytes] = mapped_column(EvmTxHash, primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    log_index: Mapped[int] = mapped_column(primary_key=True)
    destination_vault_address: Mapped[HexBytes] = mapped_column(EvmAddress, primary_key=True)

    amount: Mapped[str] = mapped_column(nullable=False)  # unscaled quantity of tokens
    owner: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)  # the autopool_vault_address, I'm pretty sure
    to_address: Mapped[HexBytes] = mapped_column(EvmAddress, nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(["tx_hash"], ["transactions.tx_hash"]),
        ForeignKeyConstraint(
            ["destination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
    )


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
    # reflect_and_create()
    drop_and_full_rebuild_db()

    pass
