# details about tokens themselves
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column, column_property
from sqlalchemy import DateTime, ForeignKeyConstraint, create_engine, String, ForeignKey, select
from sqlalchemy.dialects.postgresql import ARRAY
import pandas as pd

from mainnet_launch.database.schema.base_tables import Base, Tokens, Blocks, Transactions, Destinations



class RebalancePlan(Base):
    __tablename__ = "rebalance_plan"

    datetime_generated = Mapped[pd.Timestamp] = mapped_column(DateTime(timezone=True), nullable=False)
    autopool: Mapped[str] = mapped_column(ForeignKey("autopools.vault_address"), primary_key=True)
    solver_address: Mapped[str] = mapped_column(nullable=False)


    destinationOut: Mapped[str] = mapped_column(nullable=False)
    destinationIn: Mapped[str] = mapped_column(nullable=False)

    amount_out: Mapped[float] = mapped_column(nullable=False)
    min_amount_in: Mapped[float] = mapped_column(nullable=False)


    amount_out_safe_value: Mapped[float] = mapped_column(nullable=False)
    min_amount_in_safe_value: Mapped[float] = mapped_column(nullable=False)


    # add rank?

    # expected APR delta

    # apr in
    # apr out
    





    "solverAddress": "0x952D7a7eB2e0804d37d9244BE8e47341356d2f5D",
    "poolAddress": "0x0A2b94F6871c1D7A32Fe58E1ab5e6deA2f114E56",
    "destinationOut": "0x0A2b94F6871c1D7A32Fe58E1ab5e6deA2f114E56",
    "tokenOut": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "amountOut": "100000000000000000000",
    "amountOutETH": "100000000000000000000",
    "destinationIn": "0xE382BBd32C4E202185762eA433278f4ED9E6151E",
    "tokenIn": "0xC8Eb2Cf2f792F77AF0Cd9e203305a585E588179D",
    "minAmountIn": "100024362286265712640",
    "minAmountInETH": "99858676116613316608",