from __future__ import annotations

import pandas as pd
from multicall import Call
from concurrent.futures import ThreadPoolExecutor, as_completed


from mainnet_launch.database.schema.full import (
    AutopoolStates,
    DestinationStates,
    AutopoolDestinations,
)

from mainnet_launch.database.postgres_operations import (
    get_subset_not_already_in_column,
    insert_avoid_conflicts,
    merge_tables_as_df,
    TableSelector,
)

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    safe_normalize_6_with_bool_success,
)


from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants


def _fetch_new_autopool_state_rows(
    autopool: AutopoolConstants,
    missing_blocks: list[int],
) -> pd.DataFrame:

    def _extract_debt_plus_idle_18(success, AssetBreakdown):
        if success:
            totalIdle, totalDebt, totalDebtMin, totalDebtMax = AssetBreakdown
            return int(totalIdle + totalDebt) / 1e18

    def _extract_debt_plus_idle_6(success, AssetBreakdown):
        if success:
            totalIdle, totalDebt, totalDebtMin, totalDebtMax = AssetBreakdown
            return int(totalIdle + totalDebt) / 1e6

    calls = []
    if autopool.base_asset_decimals == 18:
        total_nav_cleaning_function = _extract_debt_plus_idle_18
        nav_per_share_cleaning_function = safe_normalize_with_bool_success
    elif autopool.base_asset_decimals == 6:
        total_nav_cleaning_function = _extract_debt_plus_idle_6
        nav_per_share_cleaning_function = safe_normalize_6_with_bool_success
    else:
        raise ValueError(f"Unknown base asset {autopool.base_asset} for autopool {autopool.autopool_eth_addr}")

    calls.extend(
        [
            Call(
                autopool.autopool_eth_addr,
                ["totalSupply()(uint256)"],
                [((autopool.autopool_eth_addr, "total_shares"), safe_normalize_with_bool_success)],  # always 1e18
            ),
            Call(
                autopool.autopool_eth_addr,
                ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
                [((autopool.autopool_eth_addr, "total_nav"), total_nav_cleaning_function)],
            ),
            Call(
                autopool.autopool_eth_addr,
                ["convertToAssets(uint256)(uint256)", int(1e18)],  # autopool shares are always in 1e18
                [((autopool.autopool_eth_addr, "nav_per_share"), nav_per_share_cleaning_function)],
            ),
        ]
    )

    autopool_state_df = get_raw_state_by_blocks(calls, missing_blocks, autopool.chain, include_block_number=True)

    new_autopool_state_rows = autopool_state_df.apply(
        lambda row: AutopoolStates(
            autopool_vault_address=autopool.autopool_eth_addr,
            block=int(row["block"]),
            chain_id=autopool.chain.chain_id,
            total_shares=row[(autopool.autopool_eth_addr, "total_shares")],
            total_nav=row[(autopool.autopool_eth_addr, "total_nav")],
            nav_per_share=row[(autopool.autopool_eth_addr, "nav_per_share")],
        ),
        axis=1,
    ).tolist()
    return new_autopool_state_rows


def _fetch_and_insert_new_autopool_states(autopool: AutopoolConstants) -> None:
    missing_blocks = _determine_what_blocks_are_needed(autopool)
    if not missing_blocks:
        return

    new_autopool_states_rows = _fetch_new_autopool_state_rows(autopool, missing_blocks)

    insert_avoid_conflicts(
        new_autopool_states_rows,
        AutopoolStates,
        index_elements=[AutopoolStates.autopool_vault_address, AutopoolStates.chain_id, AutopoolStates.block],
    )


def _determine_what_blocks_are_needed(autopool: AutopoolConstants) -> list[int]:
    # TODO rewrite in pure sql
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
        AutopoolStates,
        AutopoolStates.block,
        blocks_expected_to_have,
        where_clause=AutopoolStates.autopool_vault_address == autopool.autopool_eth_addr,
    )
    return blocks_to_fetch


def ensure_autopool_states_are_current():
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(_fetch_and_insert_new_autopool_states, ap) for ap in ALL_AUTOPOOLS]
        for fut in as_completed(futures):
            fut.result()


if __name__ == "__main__":

    from mainnet_launch.constants import *

    profile_function(ensure_autopool_states_are_current)
