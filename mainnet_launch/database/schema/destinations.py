# details about tokens themselves
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column, column_property
from sqlalchemy import DateTime, ForeignKeyConstraint, create_engine, String, ForeignKey, select
from sqlalchemy.dialects.postgresql import ARRAY
import pandas as pd

from mainnet_launch.database.schema.base_tables import Base, Tokens, Blocks, Transactions, Destinations


class DestinationStates(Base):
    __tablename__ = "destination_states"

    destination_vault_address: Mapped[str] = mapped_column(
        ForeignKey("destinations.destination_vault_address"), primary_key=True
    )
    block: Mapped[int] = mapped_column(ForeignKey("blocks.block"), primary_key=True)

    # information about the destination itself at this moment in time

    incentive_apr: Mapped[float] = mapped_column(nullable=False)
    fee_and_base_apr: Mapped[float] = mapped_column(nullable=False)
    points_apr: Mapped[float] = mapped_column(nullable=True)

    total_apr_in: Mapped[float] = mapped_column(nullable=True)
    total_apr_out: Mapped[float] = mapped_column(nullable=True)

    undelrying_token_total_staked: Mapped[float] = mapped_column(nullable=True)
    underlying_token_total_supply: Mapped[float] = mapped_column(nullable=False)

    underlying_safe_price: Mapped[float] = mapped_column(nullable=False)
    underlying_spot_price: Mapped[float] = mapped_column(nullable=False)
    underlying_backing: Mapped[float] = mapped_column(nullable=False)
    denominated_in: Mapped[str] = mapped_column(nullable=False)

    safe_backing_discount: Mapped[float] = mapped_column(nullable=True)
    safe_spot_spread: Mapped[float] = mapped_column(nullable=True)
    spot_backing_discount: Mapped[float] = mapped_column(nullable=True)


class AutopoolDestinationStates(Base):
    # information about this one autopool's assets at this destination
    __tablename__ = "autopool_destination_states"

    destination_vault_address: Mapped[str] = mapped_column(
        ForeignKey("destination_states.destination_vault_address"), primary_key=True
    )
    autopool_vault_address: Mapped[str] = mapped_column(ForeignKey("autopools.vault_address"), primary_key=True)
    block: Mapped[int] = mapped_column(ForeignKey("blocks.block"), primary_key=True)

    amount: Mapped[float] = mapped_column(nullable=False)  # how many lp tokens this autopool has here
    total_safe_value: Mapped[float] = mapped_column(nullable=False)
    total_spot_value: Mapped[float] = mapped_column(nullable=False)
    total_backing_value: Mapped[float] = mapped_column(nullable=False)

    percent_ownership: Mapped[float] = mapped_column(
        nullable=False
    )  # 100  * underlying_owned_amount / destination_states.underlying_token_total_supply
