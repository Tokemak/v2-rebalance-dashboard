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
)
from mainnet_launch.pages.autopool_diagnostics.lens_contract import fetch_pools_and_destinations_df
from mainnet_launch.constants import (
    AutopoolConstants,
    ALL_AUTOPOOLS,
    AUTO_LRT,
    POINTS_HOOK,
    ChainData,
)


def _fetch_destination_summary_stats_from_external_source(autopool:AutopoolConstants):

    # step one,
    blocks

    pass


# def fetch_destination_summary_stats(autopool: AutopoolConstants, summary_stats_field: str):
#     if summary_stats_field not in SUMMARY_STATS_FIELDS:
#         raise ValueError(f"Can only fetch {SUMMARY_STATS_FIELDS=} you tried to fetch {summary_stats_field=}")
# TODO
