from dataclasses import asdict
from dotenv import load_dotenv
from urllib.parse import urlparse
import os
import pandas as pd
import pydot


from sqlalchemy import MetaData
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

    def to_record(self) -> dict:
        return asdict(self)

    @classmethod
    def from_record(cls, record: dict):
        return cls(**record)

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
    effective_gas_price: Mapped[int] = mapped_column(BigInteger, nullable=False)
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

    __table_args__ = (ForeignKeyConstraint(["block_deployed", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


# done
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

    denominated_in: Mapped[str] = mapped_column(nullable=False)  # DestinationVaultAddress.baseAsset()

    __table_args__ = (ForeignKeyConstraint(["block_deployed", "chain_id"], ["blocks.block", "blocks.chain_id"]),)


# done
class DestinationTokens(Base):
    # missing idle destination
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
    nav_per_share: Mapped[float] = mapped_column(nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
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


# done
class DestinationStates(Base):
    __tablename__ = "destination_states"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)
    # information about the destination itself at this moment in time

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
    price_per_share: Mapped[float] = mapped_column(nullable=True)
    price_return: Mapped[float] = mapped_column(nullable=True)

    underlying_safe_price: Mapped[float] = mapped_column(nullable=True)
    underlying_spot_price: Mapped[float] = mapped_column(nullable=True)
    underlying_backing: Mapped[float] = mapped_column(nullable=True)

    safe_backing_discount: Mapped[float] = mapped_column(nullable=True)
    spot_backing_discount: Mapped[float] = mapped_column(nullable=True)
    safe_spot_spread: Mapped[float] = mapped_column(nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
        ForeignKeyConstraint(
            ["destination_vault_address", "chain_id"],
            ["destinations.destination_vault_address", "destinations.chain_id"],
        ),
    )


# done
class AutopoolDestinationStates(Base):
    # information about this one autopool's lp tokens at this destination
    __tablename__ = "autopool_destination_states"

    destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)
    chain_id: Mapped[int] = mapped_column(primary_key=True)

    amount: Mapped[float] = mapped_column(nullable=False)  # how many lp tokens this autopool has here, lens contract

    # given the value of the lp tokens in the pool how much value does the atuopool have here
    total_safe_value: Mapped[float] = mapped_column(nullable=False)
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

    denomiated_in: Mapped[str] = mapped_column(nullable=False)
    backing: Mapped[float] = mapped_column(nullable=True)
    safe_price: Mapped[float] = mapped_column(nullable=True)
    safe_backing_discount: Mapped[float] = mapped_column(nullable=True)

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


# needed
class RebalancePlan(Base):
    __tablename__ = "rebalance_plan"

    file_name: Mapped[str] = mapped_column(nullable=False, primary_key=True)

    datetime_generated: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)
    autopool_vault_address: Mapped[str] = mapped_column(nullable=False)
    chain_id: Mapped[int] = mapped_column(nullable=False)

    dex_aggregator: Mapped[str] = mapped_column(nullable=False)  # not sure here, can this be multiple

    solver_address: Mapped[str] = mapped_column(nullable=False)
    rebalance_type: Mapped[str] = mapped_column(nullable=False)

    destination_out: Mapped[str] = mapped_column(nullable=False)
    token_out: Mapped[str] = mapped_column(nullable=False)

    destination_in: Mapped[str] = mapped_column(nullable=False)
    token_in: Mapped[str] = mapped_column(nullable=False)

    move_name: Mapped[str] = mapped_column(nullable=False)  # f"{data['destinationOut']} -> {data['destinationIn']}"

    amount_out: Mapped[float] = mapped_column(nullable=False)
    # amountOutETH
    amount_out_safe_value: Mapped[float] = mapped_column(nullable=False)

    min_amount_in: Mapped[float] = mapped_column(nullable=False)
    # minAmountInETH
    min_amount_in_safe_value: Mapped[float] = mapped_column(nullable=False)

    out_spot_eth: Mapped[float] = mapped_column(nullable=False)  # in 'rebalancetTest'
    out_dest_apr: Mapped[float] = mapped_column(nullable=False)

    in_spot_eth: Mapped[float] = mapped_column(nullable=False)
    in_dest_apr: Mapped[float] = mapped_column(nullable=False)
    in_dest_adj_apr: Mapped[float] = mapped_column(nullable=False)

    apr_delta: Mapped[float] = mapped_column(nullable=False)
    swap_offset_period: Mapped[int] = mapped_column(nullable=False)

    # not certain on if this should be a list or a second table,
    # maybe make an addrank table?
    candidate_destinations: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=True)
    candidate_destinations_rank: Mapped[int] = mapped_column(nullable=False)

    projected_swap_cost: Mapped[float] = mapped_column(nullable=False)
    projected_slippage: Mapped[float] = mapped_column(nullable=False)

    # dex steps here?

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
        ForeignKeyConstraint(
            ["autopool_vault_address", "chain_id"], ["autopools.autopool_vault_address", "autopools.chain_id"]
        ),
    )


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

    # consider adding in safe and acutal total supply here?
    # only if wanted.


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


# extra
class ChainlinkGasCosts(Base):
    __tablename__ = "chainlink_gas_costs"

    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    chainlink_topic_id: Mapped[int] = mapped_column(nullable=False)
    gas_cost_in_eth_with_chainlink_premium: Mapped[float] = mapped_column(nullable=False)


def drop_and_full_rebuild_db():
    meta = MetaData()
    meta.reflect(bind=ENGINE)
    meta.drop_all(bind=ENGINE)
    print("Dropped all existing tables.")
    Base.metadata.create_all(bind=ENGINE)
    print("Recreated all tables from ORM definitions.")


import pydot
from sqlalchemy_schemadisplay import create_schema_graph

def make_schema_image():
    # 1) Build the base ERD graph
    graph = create_schema_graph(
        engine=ENGINE,
        metadata=Base.metadata,
        show_datatypes=False,
        show_indexes=False,
        rankdir="LR",
    )

    # 2) Apply your global styling
    graph.set_graph_defaults(
        splines="ortho",
        nodesep="0.6",
        ranksep="0.75",
        fontsize="12",
        dpi="300",
    )
    graph.set_node_defaults(
        shape="rectangle",
        style="filled",
        fillcolor="#f9f9f9",
        fontname="Helvetica",
    )
    graph.set_edge_defaults(
        color="#555555",
        arrowsize="0.7",
    )

    # 3) Define clusters for logical groups of tables
    #    Each cluster is a pydot.Subgraph whose name begins with "cluster_"
    autopool_tables = [
        "autopools", "autopool_states", "autopool_deposit",
        "autopool_withdrawal", "autopool_fees", "autopool_destination_states",
    ]
    dest_tables = [
        "destinations", "destination_states", "destination_tokens",
        "destination_token_values",
    ]
    rebalance_tables = ["rebalance_plan", "rebalance_events", "solver_profit"]

    def make_cluster(name, label, table_names):
        # 1) Create a true Graphviz cluster subgraph
        sub = pydot.Cluster(
            graph_name=f"cluster_{name}",  # must start with "cluster_"
            label=label,                   # cluster box title
            bgcolor="lightgrey",           # cluster background color
            style="dashed"                 # dashed border style
        )

        # 2) Add each table as a node (by name)
        for tbl in table_names:
            sub.add_node(pydot.Node(tbl))

        return sub


    graph.add_subgraph(make_cluster("autopool", "Autopools", autopool_tables))
    graph.add_subgraph(make_cluster("dest", "Destinations", dest_tables))
    graph.add_subgraph(make_cluster("rebalance", "Rebalance", rebalance_tables))

    # 4) Render to a high-res PNG
    graph.write("mainnet_launch/database/schema/schema.png", format="png")
    print("Wrote schema_clustered.png")



Session = sessionmaker(bind=ENGINE)

if __name__ == "__main__":
    make_schema_image()
