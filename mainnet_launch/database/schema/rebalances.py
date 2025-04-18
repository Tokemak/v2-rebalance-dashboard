# details about tokens themselves
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import DateTime, ForeignKey
import pandas as pd

from mainnet_launch.database.schema.base_tables import Base


class RebalancePlan(Base):
    __tablename__ = "rebalance_plan"

    file_name: Mapped[str] = mapped_column(nullable=False, primary_key=True)

    datetime_generated = Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)
    autopool: Mapped[str] = mapped_column(ForeignKey("autopools.vault_address"))
    dex_aggregator: Mapped[str] = mapped_column(nullable=False)

    solver_address: Mapped[str] = mapped_column(nullable=False)
    rebalance_type: Mapped[str] = mapped_column(nullable=False)

    destination_out: Mapped[str] = mapped_column(ForeignKey("destination_vault_address"), nullable=False)
    token_out: Mapped[str] = mapped_column(ForeignKey("tokens.address"), nullable=False)

    destination_in: Mapped[str] = mapped_column(ForeignKey("destination_vault_address"), nullable=False)
    token_in: Mapped[str] = mapped_column(ForeignKey("tokens.address"), nullable=False)

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

    candidate_destinations: Mapped[list[str]] = mapped_column(nullable=False)
    candidate_destinations_rank: Mapped[int] = mapped_column(nullable=False)

    projected_swap_cost: Mapped[float] = mapped_column(nullable=False)
    projected_slippage: Mapped[float] = mapped_column(nullable=False)


class RebalanceEvent(Base):
    __tablename__ = "rebalance_event"

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
