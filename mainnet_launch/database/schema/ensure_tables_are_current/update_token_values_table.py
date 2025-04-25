


import pandas as pd
from multicall import Call
import numpy as np
from web3 import Web3


from mainnet_launch.database.schema.full import (
    DestinationStates,
    DestinationTokenValues,
    AutopoolDestinationStates,
    Autopools,
    DestinationTokens,
    Destinations,
    Tokens,
)
from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_df,
    get_full_table_as_orm,
    insert_avoid_conflicts,
    get_highest_value_in_field_where,
    get_subset_not_already_in_column,
)
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
    get_state_by_one_block,
)
from mainnet_launch.pages.autopool_diagnostics.lens_contract import (
    fetch_active_destinations_by_autopool_by_block,
    fetch_pools_and_destinations_df,
)
from mainnet_launch.constants import (
    AutopoolConstants,
    ALL_AUTOPOOLS,
    AUTO_LRT,
    POINTS_HOOK,
    ChainData,
)


def ensure_token_values_are_current(chain:ChainData):
    # solve chains later

    possible_blocks = build_blocks_to_use(chain)

    missing_blocks = get_subset_not_already_in_column(
        DestinationStates,
        DestinationStates.block,
        possible_blocks,
        where_clause=DestinationStates.chain_id == chain.chain_id,
    )

    all_destinations_orm = get_full_table_as_orm(Destinations, where_clause=Destinations.chain_id == chain.chain_id)
    all_destination_tokens_orm = get_full_table_as_orm(DestinationTokens, where_clause=DestinationTokens.chain_id == chain.chain_id)
    all_tokens_orm = get_full_table_as_orm(Tokens, where_clause=Tokens.chain_id == chain.chain_id)


    pass

def build_safe_price_calls(tokens:list[Tokens]) -> pd.DataFrame:
    





# #class TokenValues(Base):
#     __tablename__ = "token_values"

#     block: Mapped[int] = mapped_column(primary_key=True)
#     chain_id: Mapped[int] = mapped_column(primary_key=True)
#     token_address: Mapped[str] = mapped_column(primary_key=True)

#     denomiated_in: Mapped[str] = mapped_column(nullable=False)
#     backing: Mapped[float] = mapped_column(nullable=True)
#     safe_price: Mapped[float] = mapped_column(nullable=True)

#     __table_args__ = (
#         # link back to blocks
#         ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
#         # composite FK into tokens(address, chain_id)
#         ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.address", "tokens.chain_id"]),
#     )

# class DestinationTokenValues(Base):
#     __tablename__ = "destination_token_values"

#     block: Mapped[int] = mapped_column(primary_key=True)
#     chain_id: Mapped[int] = mapped_column(primary_key=True)
#     token_address: Mapped[str] = mapped_column(nullable=False)
#     destination_address: Mapped[str] = mapped_column(nullable=False)

#     spot_price: Mapped[float] = mapped_column(nullable=True)
#     quantity: Mapped[float] = mapped_column(nullable=False)
#     safe_spot_spread: Mapped[float] = mapped_column(nullable=True)
#     spot_backing_discount: Mapped[float] = mapped_column(nullable=True)

#     __table_args__ = (
#         # point (block, chain_id) → blocks
#         ForeignKeyConstraint(["block", "chain_id"], ["blocks.block", "blocks.chain_id"]),
#         # composite FK into tokens(address, chain_id)
#         ForeignKeyConstraint(["token_address", "chain_id"], ["tokens.address", "tokens.chain_id"]),
#         # composite FK into destinations(destination_vault_address, chain_id)
#         ForeignKeyConstraint(
#             ["destination_address", "chain_id"], ["destinations.destination_vault_address", "destinations.chain_id"]
#         ),
#     )
