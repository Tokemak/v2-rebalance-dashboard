# # not populated
# class SolverProfit(Base):
#     __tablename__ = "solver_profit"

#     tx_hash: Mapped[str] = mapped_column(ForeignKey("rebalace_events.tx_hash"), primary_key=True)
#     block: Mapped[int] = mapped_column(primary_key=True)
#     chain_id: Mapped[int] = mapped_column(primary_key=True)

#     denominated_in: Mapped[str] = mapped_column(nullable=False)

#     solver_value_held_before_rebalance: Mapped[float] = mapped_column(nullable=False)
#     solver_value_held_after_rebalance: Mapped[float] = mapped_column(nullable=False)

#     __table_args__ = (ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),)
