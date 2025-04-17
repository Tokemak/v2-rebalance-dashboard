# details about tokens themselves
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import ForeignKey


class SolverGasCosts:

    __tablename__ = "solver_gas_costs"

    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    autopool: Mapped[str] = mapped_column(ForeignKey("autopools.vault_address"), primary_key=True)
    flash_borrow_solver: Mapped[str] = mapped_column(nullable=False)


class ChainlinkGasCosts:

    __tablename__ = "chainlink_gas_costs"

    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
    chainlink_topic_id: Mapped[int] = mapped_column(nullable=False)
    chainlink_topic_id: Mapped[str] = mapped_column(nullable=False)
    # maybe we want it in USD?
    gas_cost_in_eth_with_chainlink_premium: Mapped[float] = mapped_column(nullable=False)


class DebtReportingGasCosts:

    __tablename__ = "debt_reporting_gas_costs"

    tx_hash: Mapped[str] = mapped_column(ForeignKey("transactions.tx_hash"), primary_key=True)
