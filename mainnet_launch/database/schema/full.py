# The primary objects
from dataclasses import asdict
from dotenv import load_dotenv
from urllib.parse import urlparse
import os
import pandas as pd


from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column
from sqlalchemy import DateTime, ForeignKey, ARRAY, String, ForeignKeyConstraint, BigInteger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


load_dotenv()

tmpPostgres = urlparse(os.getenv("DEV_LOCAL_DATABASE_URL"))

ENGINE = create_engine(
    f"postgresql+psycopg2://{tmpPostgres.username}:{tmpPostgres.password}"
    f"@{tmpPostgres.hostname}{tmpPostgres.path}?sslmode=require",
    echo=True,  # Enable SQL query logging for debugging.
)


class Base(MappedAsDataclass, DeclarativeBase):
    """subclasses will be converted to dataclasses"""

    def to_record(self):
        return asdict(self)


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


class Tokens(Base):
    __tablename__ = "tokens"

    address: Mapped[str] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    reference_asset: Mapped[str] = mapped_column(nullable=True)  # ETH? USDC? pxETH? None, for CRV / BAL


class Autopools(Base):
    __tablename__ = "autopools"
    vault_address: Mapped[str] = mapped_column(primary_key=True)

    block_deployed: Mapped[int] = mapped_column(nullable=False)
    chain_id: Mapped[int] = mapped_column(nullable=False)

    name: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)
    strategy_address: Mapped[str] = mapped_column(nullable=True)
    # not certain if the strategy address can be changed
    base_asset: Mapped[str] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block_deployed", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class Destinations(Base):
    __tablename__ = "destinations"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)

    block_deployed: Mapped[int] = mapped_column(nullable=False)
    chain_id: Mapped[int] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)
    exchange_name: Mapped[str] = mapped_column(nullable=False)
    pool: Mapped[str] = mapped_column(nullable=False)
    underlying: Mapped[str] = mapped_column(nullable=False)
    # not certain here on the underlying ARRAY format,

    __table_args__ = (ForeignKeyConstraint(["block_deployed", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class DestinationTokens(Base):
    __tablename__ = "destination_tokens"

    destination_vault_address: Mapped[str] = mapped_column(
        ForeignKey("destinations.destination_vault_address"), primary_key=True
    )
    underlying_asset: Mapped[str] = mapped_column(ForeignKey("tokens.address"), primary_key=True)
    index: Mapped[int] = mapped_column(nullable=False)


class AutopoolStates(Base):
    __tablename__ = "autopool_states"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(ForeignKey("autopools.vault_address"), primary_key=True)

    total_shares: Mapped[float] = mapped_column(nullable=False)
    total_nav: Mapped[float] = mapped_column(nullable=False)
    nav_per_share: Mapped[float] = mapped_column(nullable=False)

    weighted_average_total_apr_out: Mapped[float] = mapped_column(nullable=False)
    weighted_average_total_apr_in: Mapped[float] = mapped_column(nullable=False)
    weighted_average_safe_backing_discount: Mapped[float] = mapped_column(nullable=False)  # price return

    active_destinations: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class AutopoolTokenStates(Base):
    __tablename__ = "autopool_token_states"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(ForeignKey("autopools.vault_address"), primary_key=True)
    token_address: Mapped[str] = mapped_column(ForeignKey("tokens.address"), primary_key=True)

    amount: Mapped[float] = mapped_column(nullable=False)
    total_safe_value: Mapped[float] = mapped_column(nullable=False)
    total_spot_value: Mapped[float] = mapped_column(nullable=False)
    total_backing_value: Mapped[float] = mapped_column(nullable=False)

    # feature does not exist
    # some way of measuing how much of total liquidity is owned by this autopool
    # how much do we get out if we try and sell everything here

    # how good this quote is for
    dex_aggregator_init_datetime: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)
    dex_aggregator_cutoff_datetime: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)

    dex_aggregator_quote_to_base_asset_1_percent: Mapped[float] = mapped_column(nullable=False)
    dex_aggregator_quote_to_base_asset_10_percent: Mapped[float] = mapped_column(nullable=False)
    dex_aggregator_quote_to_base_asset_33_percent: Mapped[float] = mapped_column(nullable=False)
    dex_aggregator_quote_to_base_asset_50_percent: Mapped[float] = mapped_column(nullable=False)
    dex_aggregator_quote_to_base_asset_100_percent: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class AutopoolDeposit(Base):
    __tablename__ = "autopool_deposit"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(ForeignKey("autopools.vault_address"), primary_key=True)

    shares: Mapped[float] = mapped_column(nullable=False)
    base_asset_amount: Mapped[float] = mapped_column(nullable=False)  # quantity of (WETH) or USDC or pxETH

    user: Mapped[str] = mapped_column(nullable=False)
    nav_per_share: Mapped[str] = mapped_column(nullable=False)


class AutopoolWithdrawal(Base):
    __tablename__ = "autopool_withdrawal"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(ForeignKey("autopools.vault_address"), primary_key=True)

    shares: Mapped[float] = mapped_column(nullable=False)
    base_asset_amount: Mapped[float] = mapped_column(nullable=False)  # quantity of (WETH) or USDC or pxETH

    user: Mapped[str] = mapped_column(nullable=False)
    nav_per_share: Mapped[float] = mapped_column(nullable=False)  # on deposit

    actualized_nav_per_share: Mapped[float] = mapped_column(
        nullable=False
    )  # the actual ratio of base asset amount / shares they got out
    slippage: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class AutopoolFees(Base):
    __tablename__ = "autopool_fees"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(ForeignKey("autopools.vault_address"), primary_key=True)

    event_name: Mapped[str] = mapped_column(nullable=False)

    denominated_in: Mapped[str] = mapped_column(nullable=False)
    minted_shares: Mapped[float] = mapped_column(nullable=False)
    minted_shares_value: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class DestinationStates(Base):
    __tablename__ = "destination_states"

    destination_vault_address: Mapped[str] = mapped_column(
        ForeignKey("destinations.destination_vault_address"), primary_key=True
    )
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    # information about the destination itself at this moment in time

    incentive_apr: Mapped[float] = mapped_column(nullable=False)
    fee_and_base_apr: Mapped[float] = mapped_column(nullable=False)
    points_apr: Mapped[float] = mapped_column(nullable=True)

    total_apr_in: Mapped[float] = mapped_column(nullable=True)
    total_apr_out: Mapped[float] = mapped_column(nullable=True)

    undelrying_token_total_staked: Mapped[float] = mapped_column(nullable=True)
    underlying_token_total_supply: Mapped[float] = mapped_column(nullable=False)
    safe_total_supply: Mapped[float] = mapped_column(nullable=True)  # only for pre autoUSD destinations

    underlying_safe_price: Mapped[float] = mapped_column(nullable=False)
    underlying_spot_price: Mapped[float] = mapped_column(nullable=False)
    underlying_backing: Mapped[float] = mapped_column(nullable=False)
    denominated_in: Mapped[str] = mapped_column(nullable=False)

    safe_backing_discount: Mapped[float] = mapped_column(nullable=True)
    safe_spot_spread: Mapped[float] = mapped_column(nullable=True)
    spot_backing_discount: Mapped[float] = mapped_column(nullable=True)

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class AutopoolDestinationStates(Base):
    # information about this one autopool's lp tokens at this destination
    __tablename__ = "autopool_destination_states"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(ForeignKey("autopools.vault_address"), primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    amount: Mapped[float] = mapped_column(nullable=False)  # how many lp tokens this autopool has here
    total_safe_value: Mapped[float] = mapped_column(nullable=False)
    total_spot_value: Mapped[float] = mapped_column(nullable=False)
    total_backing_value: Mapped[float] = mapped_column(nullable=False)

    percent_ownership: Mapped[float] = mapped_column(
        nullable=False
    )  # 100  * underlying_owned_amount / destination_states.underlying_token_total_supply

    __table_args__ = (
        ForeignKeyConstraint(
            ["destination_vault_address", "block", "chain_id"],
            ["destination_states.destination_vault_address", "destination_states.block", "destination_states.chain_id"],
        ),
    )


class DebtReporting(Base):
    __tablename__ = "debt_reporting"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(ForeignKey("autopools.vault_address"), primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), nullable=False)

    denominated_in: Mapped[str] = mapped_column(nullable=False)
    base_asset_value: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["destination_vault_address", "block", "chain_id"],
            ["destination_states.destination_vault_address", "destination_states.block", "destination_states.chain_id"],
        ),
    )


class ChainlinkGasCosts(Base):
    __tablename__ = "chainlink_gas_costs"

    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    chainlink_topic_id: Mapped[int] = mapped_column(nullable=False)
    gas_cost_in_eth_with_chainlink_premium: Mapped[float] = mapped_column(nullable=False)


class RebalancePlan(Base):
    __tablename__ = "rebalance_plan"

    file_name: Mapped[str] = mapped_column(nullable=False, primary_key=True)

    datetime_generated: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)
    autopool: Mapped[str] = mapped_column(ForeignKey("autopools.vault_address"))
    dex_aggregator: Mapped[str] = mapped_column(nullable=False)

    solver_address: Mapped[str] = mapped_column(nullable=False)
    rebalance_type: Mapped[str] = mapped_column(nullable=False)

    # sometimes this has different destinations but the same underlying token. that means
    destination_out: Mapped[str] = mapped_column(ForeignKey("destinations.destination_vault_address"), nullable=False)
    token_out: Mapped[str] = mapped_column(ForeignKey("tokens.address"), nullable=False)

    destination_in: Mapped[str] = mapped_column(ForeignKey("destinations.destination_vault_address"), nullable=False)
    token_in: Mapped[str] = mapped_column(ForeignKey("tokens.address"), nullable=False)

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

    # dex steps here?


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


class SolverProfit(Base):
    __tablename__ = "solver_profit"

    tx_hash: Mapped[str] = mapped_column(ForeignKey("rebalance_events.tx_hash"), primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    denominated_in: Mapped[str] = mapped_column(nullable=False)

    solver_value_held_before_rebalance: Mapped[float] = mapped_column(nullable=False)
    solver_value_held_after_rebalance: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class TokenValues(Base):
    # if the same token symbol can have different values on different chains at the same time
    __tablename__ = "token_values"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    tokenAddress: Mapped[str] = mapped_column(ForeignKey("tokens.address"), primary_key=True)

    denomiated_in: Mapped[str] = mapped_column(nullable=False)
    backing: Mapped[float] = mapped_column(nullable=True)
    safe_price: Mapped[float] = mapped_column(nullable=True)

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class DestinationTokenValues(Base):
    # information about one token in a destination

    __tablename__ = "destination_token_values"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    token_address: Mapped[str] = mapped_column(ForeignKey("tokens.address"), primary_key=True)
    destination_address: Mapped[str] = mapped_column(primary_key=True)

    spot_price: Mapped[float] = mapped_column(nullable=True)
    quantity: Mapped[float] = mapped_column(nullable=False)

    safe_spot_spread: Mapped[float] = mapped_column(nullable=True)
    spot_backing_discount: Mapped[float] = mapped_column(nullable=True)

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


class IncentiveTokenLiquidations(Base):
    # information about sold incentive tokens

    __tablename__ = "incentive_token_liquidations"

    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    token_address: Mapped[str] = mapped_column(ForeignKey("tokens.address"), primary_key=True)
    destination_address: Mapped[str] = mapped_column(primary_key=True)  # what destination this token is sold for

    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), nullable=False)

    acheived_price: Mapped[float] = mapped_column(nullable=False)
    safe_price: Mapped[float] = mapped_column(nullable=True)  # points to tokens values
    incentive_calculator_price: Mapped[float] = mapped_column(nullable=False)

    buy_amount: Mapped[float] = mapped_column(nullable=False)
    sell_amount: Mapped[float] = mapped_column(nullable=False)

    denominated_in: Mapped[str] = mapped_column(nullable=False)

    incentive_calculator_price_diff_with_acheived: Mapped[float] = mapped_column(nullable=False)
    safe_price_diff_with_acheived: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


Session = sessionmaker(bind=ENGINE)
if __name__ == "__main__":
    from sqlalchemy import MetaData

    # 1) Reflect the *actual* database schema
    meta = MetaData()
    meta.reflect(bind=ENGINE)

    # 2) Drop *all* tables that exist in the DB right now
    meta.drop_all(bind=ENGINE)
    print("Dropped all existing tables.")

    # 3) Create *only* the tables you have declared on Base
    Base.metadata.create_all(bind=ENGINE)
    print("Recreated all tables from ORM definitions.")
