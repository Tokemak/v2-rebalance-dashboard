# details about tokens themselves
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import DateTime, ForeignKey
import pandas as pd

from mainnet_launch.database.schema.base_tables import Base


class AutopoolStates(Base):
    __tablename__ = "autopool_states"

    block: Mapped[int] = mapped_column(ForeignKey("blocks.block"), primary_key=True, nullable=False)
    autopool_vault_address: Mapped[str] = mapped_column(ForeignKey("autopools.vault_address"), primary_key=True)

    total_shares: Mapped[float] = mapped_column(nullable=False)
    total_nav: Mapped[float] = mapped_column(nullable=False)
    nav_per_share: Mapped[float] = mapped_column(nullable=False)

    weighted_average_total_apr_out: Mapped[float] = mapped_column(nullable=False)
    weighted_average_total_apr_in: Mapped[float] = mapped_column(nullable=False)
    weighted_average_safe_backing_discount: Mapped[float] = mapped_column(nullable=False)

    active_destinations: Mapped[list[str]] = mapped_column(nullable=False)


class AutopoolTokenStates(Base):
    __tablename__ = "autopool_token_states"

    block: Mapped[int] = mapped_column(ForeignKey("blocks.block"), primary_key=True, nullable=False)
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


# move elsewhere?
class AuotpoolFees(Base):
    __tablename__ = "autopool_fees"

    block: Mapped[int] = mapped_column(ForeignKey("blocks.block"), nullable=False)
    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    autopool_vault_address: Mapped[str] = mapped_column(ForeignKey("autopools.vault_address"), primary_key=True)
    amount: Mapped[float] = mapped_column(nullable=False)
