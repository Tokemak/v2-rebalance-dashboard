# import pandas as pd
# from multicall import Call
# import numpy as np
# from web3 import Web3


# from mainnet_launch.database.schema.full import Tokens, TokenValues, DestinationTokenValues, Autopools, DestinationStates, DestinationTokens, Destinations, AutopoolDestinationStates


# from mainnet_launch.abis import STATS_CALCULATOR_REGISTRY_ABI
# from mainnet_launch.data_fetching.get_events import fetch_events


# from mainnet_launch.database.schema.postgres_operations import (
#     get_full_table_as_orm,
#     get_full_table_as_df,
#     insert_avoid_conflicts,
#     get_subset_not_already_in_column,
#     natural_left_right_using_where
# )
# from mainnet_launch.data_fetching.get_state_by_block import (
#     get_raw_state_by_blocks,
#     safe_normalize_with_bool_success,
#     build_blocks_to_use,
#     identity_with_bool_success,
#     get_state_by_one_block,
# )
# from mainnet_launch.constants import ALL_CHAINS, ROOT_PRICE_ORACLE, ChainData, STATS_CALCULATOR_REGISTRY, WETH


# def ensure_autopool_destination_state_is_current():
#     for chain in ALL_CHAINS:
#         # consider moving this out to a seperate method
#         possible_blocks = build_blocks_to_use(chain)

#         missing_blocks = get_subset_not_already_in_column(
#             AutopoolDestinationStates,
#             AutopoolDestinationStates.block,
#             possible_blocks,
#             where_clause=AutopoolDestinationStates.chain_id == chain.chain_id,
#         )
#         if len(missing_blocks) == 0:
#             continue


#         autopool_df = get_full_table_as_df(Autopools, where_clause=Autopools.chain_id == chain.chain_id)
#             full_destination_df = natural_left_right_using_where(
#             DestinationTokens,
#             Destinations,
#             using=[DestinationTokens.destination_vault_address, DestinationTokens.chain_id],
#             where_clause=DestinationTokens.chain_id == chain.chain_id,
#         )

#         token_value_df = natural_left_right_using_where(
#             DestinationTokenValues,
#             TokenValues,
#             using=[DestinationTokenValues.block, DestinationTokens.chain_id,DestinationTokens.token_address],
#             where_clause=DestinationTokenValues.chain_id == chain.chain_id,
#         )


# # class AutopoolDestinationStates(Base):
# #     # information about this one autopool's lp tokens at this destination
# #     __tablename__ = "autopool_destination_states"

# #     destination_vault_address: Mapped[str] = mapped_column(primary_key=True)
# #     autopool_vault_address: Mapped[str] = mapped_column(primary_key=True)
# #     block: Mapped[int] = mapped_column(primary_key=True)
# #     chain_id: Mapped[int] = mapped_column(primary_key=True)

#         # see lense contract for (autopool, active destinations)


# #     amount: Mapped[float] = mapped_column(nullable=False)  # how many lp tokens this autopool has here, lens contract
# #     total_safe_value: Mapped[float] = mapped_column(
# #         nullable=False
# #     )  # given the value of the lp tokens in the pool how much value does the atuopool have here
# #     total_spot_value: Mapped[float] = mapped_column(nullable=False)
# #     total_backing_value: Mapped[float] = mapped_column(nullable=False)

# #     percent_ownership: Mapped[float] = mapped_column(
# #         nullable=False
# #     )  # 100  * underlying_owned_amount / destination_states.underlying_token_total_supply

# #     __table_args__ = (
# #         ForeignKeyConstraint(
# #             ["destination_vault_address", "block", "chain_id"],
# #             ["destination_states.destination_vault_address", "destination_states.block", "destination_states.chain_id"],
# #         ),
# #         ForeignKeyConstraint(["autopool_vault_address", "chain_id"], ["autopools.vault_address", "autopools.chain_id"]),
# #     )


#         # underlying.totalSupply() vault ot get the balance
#         # undelrygin .balanceOf(autopools) to ge the quanityt of lp tokens


#         # token_value_df = natural_left_right_using_where(
#         #     TokenValues,
#         #     Tokens,
#         #     using=[TokenValues.token_address, TokenValues.chain_id],
#         #     where_clause=TokenValues.chain_id == chain.chain_id,
#         # )
#         # wide_df = _fetch_destination_token_value_data_from_external_source(chain, missing_blocks, full_destination_df)

#         # all_destination_token_values = _build_all_destination_token_values(
#         #     chain, full_destination_df, wide_df, token_value_df
#         # )
#         # insert_avoid_conflicts(
#         #     all_destination_token_values,
#         #     DestinationTokenValues,
#         #     index_elements=[
#         #         DestinationTokenValues.block,
#         #         DestinationTokenValues.chain_id,
#         #         DestinationTokenValues.token_address,
#         #         DestinationTokenValues.destination_vault_address,
#         #     ],
#         # )
