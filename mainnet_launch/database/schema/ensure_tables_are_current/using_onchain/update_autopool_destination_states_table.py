from multicall import Call
import pandas as pd

from mainnet_launch.database.schema.full import (
    DestinationTokenValues,
    AutopoolDestinationStates,
    DestinationStates,
    AutopoolDestinations,
    Destinations,
)
from mainnet_launch.database.schema.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    merge_tables_as_df,
    TableSelector,
)
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    get_state_by_one_block,
    safe_normalize_with_bool_success,
    safe_normalize_6_with_bool_success,
)
from mainnet_launch.constants import (
    ALL_CHAINS,
    ChainData,
    ALL_AUTOPOOLS,
    AutopoolConstants,
)


def _determine_what_blocks_are_needed(autopool: AutopoolConstants) -> list[int]:
    blocks_expected_to_have = merge_tables_as_df(
        selectors=[
            TableSelector(
                AutopoolDestinations,
                [
                    AutopoolDestinations.destination_vault_address,
                    AutopoolDestinations.autopool_vault_address,
                ],
            ),
            TableSelector(
                DestinationStates,
                DestinationStates.block,
                join_on=(DestinationStates.destination_vault_address == AutopoolDestinations.destination_vault_address),
            ),
        ],
        where_clause=(DestinationStates.chain_id == autopool.chain.chain_id)
        & (AutopoolDestinations.autopool_vault_address == autopool.autopool_eth_addr),
    )["block"].unique()

    blocks_to_fetch = get_subset_not_already_in_column(
        AutopoolDestinationStates,
        AutopoolDestinationStates.block,
        blocks_expected_to_have,
        where_clause=(AutopoolDestinationStates.autopool_vault_address == autopool.autopool_eth_addr),
    )

    # blocks_to_fetch = [int(b) for b in blocks_to_fetch]

    return [int(b) for b in blocks_expected_to_have]


def _fetch_and_insert_new_autopool_destination_states(autopool: AutopoolConstants):
    missing_blocks = _determine_what_blocks_are_needed(autopool)
    if len(missing_blocks) == 0:
        return

    destination_info_df = merge_tables_as_df(
        selectors=[
            TableSelector(
                AutopoolDestinations,
                [
                    AutopoolDestinations.destination_vault_address,
                    AutopoolDestinations.autopool_vault_address,
                ],
            ),
            TableSelector(
                Destinations,
                [Destinations.destination_vault_decimals],
                join_on=AutopoolDestinations.destination_vault_address == Destinations.destination_vault_address,
            ),
        ],
        where_clause=(
            (AutopoolDestinations.chain_id == autopool.chain.chain_id)
            & (AutopoolDestinations.autopool_vault_address == autopool.autopool_eth_addr)
            & (AutopoolDestinations.autopool_vault_address != AutopoolDestinations.destination_vault_address)
        ),
    )

    balance_of_calls = []

    for destination_vault_address, decimals in zip(
        destination_info_df["destination_vault_address"],
        destination_info_df["destination_vault_decimals"],
    ):
        if decimals == 18:
            cleaning_function = safe_normalize_with_bool_success
        elif decimals == 6:
            cleaning_function = safe_normalize_6_with_bool_success

        balance_of_calls.append(
            Call(
                destination_vault_address,
                ["balanceOf(address)(uint256)", autopool.autopool_eth_addr],
                [(destination_vault_address, cleaning_function)],
            )
        )

    if autopool.base_asset_decimals == 18:
        cleaning_function = safe_normalize_with_bool_success
    elif autopool.base_asset_decimals == 6:
        cleaning_function = safe_normalize_6_with_bool_success

    # add in the balance of idle here as well
    balance_of_calls.append(
        Call(
            autopool.base_asset,
            ["balanceOf(address)(uint256)", autopool.autopool_eth_addr],
            [(autopool.autopool_eth_addr, cleaning_function)],
        )
    )
    # we know for certain that idle is correct (autopoool.auto_eth_addr: float(idle)

    autopool_destination_balance_of_df = get_raw_state_by_blocks(
        balance_of_calls, missing_blocks, autopool.chain, include_block_number=True
    )
    # how much of each destination does this autopool have in shares at this block

    new_autopool_destination_states_rows = []

    def _extract_autopool_destination_vault_balance_of_block(row: dict):

        for destination_vault_address in [
            *destination_info_df["destination_vault_address"].values,
            autopool.autopool_eth_addr,
        ]:
            owned_shares = float(row[destination_vault_address])

            if destination_vault_address == autopool.autopool_eth_addr:
                pass

            new_autopool_destination_states_rows.append(
                AutopoolDestinationStates(
                    destination_vault_address=destination_vault_address,
                    autopool_vault_address=autopool.autopool_eth_addr,
                    block=int(row["block"]),
                    chain_id=autopool.chain.chain_id,
                    owned_shares=owned_shares,
                )
            )

    autopool_destination_balance_of_df.apply(_extract_autopool_destination_vault_balance_of_block, axis=1)

    insert_avoid_conflicts(
        new_autopool_destination_states_rows,
        AutopoolDestinationStates,
    )


def ensure_autopool_destination_states_are_current():
    for autopool in ALL_AUTOPOOLS:
        _fetch_and_insert_new_autopool_destination_states(autopool)


if __name__ == "__main__":
    ensure_autopool_destination_states_are_current()
    # _determine_what_blocks_are_needed(ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN, ALL_CHAINS[0])
