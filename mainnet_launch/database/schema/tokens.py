# details about tokens themselves
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column, column_property
from sqlalchemy import DateTime, ForeignKeyConstraint, create_engine, String, ForeignKey, select
from sqlalchemy.dialects.postgresql import ARRAY
import pandas as pd

from mainnet_launch.database.schema.base_tables import Base, Tokens, Blocks, Transactions, Destinations


# option 1:
# backing and safe price at a moment in time (universal)
# this assumes equal values of the same token across chains
class GlobalTokenValuesSymbolTime:

    __tablename__ = "global_token_values_symbol_time"

    symbol: Mapped[str] = mapped_column(primary_key=True)
    datetime: Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), primary_key=True)

    # WETH, USDC pxETH etc
    denominated_in: Mapped[str] = mapped_column(nullable=False)
    backing: Mapped[float] = mapped_column(nullable=True)
    safe_price: Mapped[float] = mapped_column(nullable=True)
    safe_backing_discount: Mapped[float] = mapped_column(nullable=True)


class GlobalTokenValuesChainAddressBlock:
    # if the same token symbol can have different values on different chains at the same time
    __tablename__ = "global_token_values_chain_address_block"

    block: Mapped[int] = mapped_column(ForeignKey("blocks.block"), primary_key=True)
    chain_id: Mapped[int] = mapped_column(ForeignKey("blocks.chain_id"), primary_key=True, nullable=False)
    tokenAddress: Mapped[str] = mapped_column(ForeignKey("tokens.address"), primary_key=True)

    denomiated_in: Mapped[str] = mapped_column(nullable=False)
    backing: Mapped[float] = mapped_column(nullable=True)
    safe_price: Mapped[float] = mapped_column(nullable=True)


class DestinationTokenValues(Base):
    # information about one token in a destination

    __tablename__ = "destination_token_values"

    token_address: Mapped[str] = mapped_column(primary_key=True)
    destination_address: Mapped[str] = mapped_column(primary_key=True)
    block: Mapped[int] = mapped_column(primary_key=True)

    spot_price: Mapped[float] = mapped_column(nullable=True)
    quantity: Mapped[float] = mapped_column(nullable=False)

    safe_spot_spread: Mapped[float] = mapped_column(nullable=True)
    spot_backing_discount: Mapped[float] = mapped_column(nullable=True)
