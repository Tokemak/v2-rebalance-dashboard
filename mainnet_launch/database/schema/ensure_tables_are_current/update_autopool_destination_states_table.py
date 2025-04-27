import pandas as pd
from multicall import Call
import numpy as np
from web3 import Web3


from mainnet_launch.database.schema.full import (
    DestinationTokenValues,
    TokenValues,
    Autopools,
    DestinationStates,
    DestinationTokens,
    Destinations,
    AutopoolDestinationStates,
    Tokens,
)
import plotly.express as px


from mainnet_launch.abis import STATS_CALCULATOR_REGISTRY_ABI
from mainnet_launch.data_fetching.get_events import fetch_events


from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
    get_full_table_as_df,
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    natural_left_right_using_where,
)
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
    identity_with_bool_success,
    get_state_by_one_block,
)
from mainnet_launch.constants import (
    ALL_CHAINS,
    ROOT_PRICE_ORACLE,
    ChainData,
)

from mainnet_launch.pages.autopool_diagnostics.lens_contract import (
    fetch_autopool_to_active_destinations_over_this_period_of_missing_blocks,
)


import pandas as pd
from multicall import Call
import numpy as np


from mainnet_launch.database.schema.full import (
    DestinationStates,
    AutopoolDestinationStates,
    Destinations,
)
from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_df,
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
)
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
)

from mainnet_launch.constants import (
    ChainData,
)


def build_autopool_balance_of_calls_by_destination(
    autopool_vault_address: str, destination_vault_addresses: list[str]
) -> list[Call]:
    return [
        Call(
            destination_vault_address,
            ["balanceOf(address)(uint256)", autopool_vault_address],
            [((autopool_vault_address, destination_vault_address, "balanceOf"), safe_normalize_with_bool_success)],
        )
        for destination_vault_address in destination_vault_addresses
    ]


def fetch_autopool_balance_of_by_destination(
    autopool_to_all_ever_active_destinations: dict[str, list[Destinations]], missing_blocks: list[int], chain: ChainData
) -> pd.DataFrame:
    autopool_balance_of_calls = []

    for autopool_vault_address in autopool_to_all_ever_active_destinations.keys():
        this_autopool_active_destinations = [
            dest.destination_vault_address for dest in autopool_to_all_ever_active_destinations[autopool_vault_address]
        ]

        autopool_balance_of_calls.extend(
            build_autopool_balance_of_calls_by_destination(autopool_vault_address, this_autopool_active_destinations)
        )

    autopool_destination_balance_of_df = get_raw_state_by_blocks(
        autopool_balance_of_calls, missing_blocks, chain, include_block_number=True
    )

    autopool_destination_balance_of_records = []

    def _extract_autopool_destination_vault_balance_of_block(row: dict):
        for k in row.keys():
            if k != "block":
                autopool_vault_address, destination_vault_address, _ = k
                balance_of = row[k]
                autopool_destination_balance_of_records.append(
                    {
                        "block": row["block"],
                        "autopool_vault_address": autopool_vault_address,
                        "destination_vault_address": destination_vault_address,
                        "balance_of": balance_of,
                    }
                )

    autopool_destination_balance_of_df.apply(_extract_autopool_destination_vault_balance_of_block, axis=1)
    return pd.DataFrame.from_records(autopool_destination_balance_of_records)


def _extract_new_autopool_destination_state_rows(
    destination_states_df: pd.DataFrame, autopool_destination_balance_of_df: pd.DataFrame, chain: ChainData
):
    limited_destination_states_df = destination_states_df[
        [
            "destination_vault_address",
            "block",
            "underlying_token_total_supply",
            "underlying_safe_price",
            "underlying_spot_price",
            "underlying_backing",
        ]
    ].copy()
    raw_autopool_destination_state_df = pd.merge(
        limited_destination_states_df, autopool_destination_balance_of_df, on=["block", "destination_vault_address"]
    )

    new_autopool_destination_state_rows = []

    def _extract_autopool_destination_state(row: dict) -> None:
        new_autopool_destination_state_rows.append(
            AutopoolDestinationStates(
                destination_vault_address=row["destination_vault_address"],
                autopool_vault_address=row["autopool_vault_address"],
                block=row["block"],
                chain_id=chain.chain_id,
                amount=row["balance_of"],
                total_safe_value=row["balance_of"] * row["underlying_safe_price"],
                total_spot_value=row["balance_of"] * row["underlying_spot_price"],
                total_backing_value=row["balance_of"] * row["underlying_backing"],
                percent_ownership=100 * (row["balance_of"] / row["underlying_token_total_supply"]),
            )
        )

    raw_autopool_destination_state_df.apply(_extract_autopool_destination_state, axis=1)

    return new_autopool_destination_state_rows


def ensure_autopool_destination_states_is_current():
    for chain in ALL_CHAINS:
        possible_blocks = build_blocks_to_use(chain)

        missing_blocks = get_subset_not_already_in_column(
            AutopoolDestinationStates,
            AutopoolDestinationStates.block,
            possible_blocks,
            where_clause=AutopoolDestinationStates.chain_id == chain.chain_id,
        )

        if len(missing_blocks) == 0:
            continue

        autopool_to_all_ever_active_destinations = (
            fetch_autopool_to_active_destinations_over_this_period_of_missing_blocks(chain, missing_blocks)
        )

        autopool_destination_balance_of_df = fetch_autopool_balance_of_by_destination(
            autopool_to_all_ever_active_destinations, missing_blocks, chain
        )

        destination_states_df = get_full_table_as_df(
            DestinationStates, where_clause=DestinationStates.chain_id == chain.chain_id
        )

        new_autopool_destination_state_rows = _extract_new_autopool_destination_state_rows(
            destination_states_df, autopool_destination_balance_of_df, chain
        )

        insert_avoid_conflicts(
            new_autopool_destination_state_rows,
            AutopoolDestinationStates,
            index_elements=[
                AutopoolDestinationStates.destination_vault_address,
                AutopoolDestinationStates.autopool_vault_address,
                AutopoolDestinationStates.block,
                AutopoolDestinationStates.chain_id,
            ],
        )


if __name__ == "__main__":
    ensure_autopool_destination_states_is_current()
