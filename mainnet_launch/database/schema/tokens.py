from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import ForeignKey

from mainnet_launch.database.schema.base_tables import Base


class TokenValues:
    # if the same token symbol can have different values on different chains at the same time
    __tablename__ = "token_values"

    block: Mapped[int] = mapped_column(ForeignKey("blocks.block"), primary_key=True)
    chain_id: Mapped[int] = mapped_column(ForeignKey("blocks.chain_id"), primary_key=True)
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
