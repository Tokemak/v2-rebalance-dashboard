import pandas as pd
from multicall import Call

from mainnet_launch.database.schema.full import (
    Autopools,
    AutopoolStates,
    DestinationStates,
    AutopoolDestinations,
)

from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
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


from mainnet_launch.constants import (
    ALL_CHAINS,
    ChainData,
    USDC,
    WETH,
    DOLA,
    AutopoolConstants,
    ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN,
    ALL_AUTOPOOLS_DATA_ON_CHAIN,
)


def _fetch_new_autopool_state_rows(
    autopools: list[AutopoolConstants], missing_blocks: list[int], chain: ChainData
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
    for autopool in autopools:
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

    autopool_state_df = get_raw_state_by_blocks(calls, missing_blocks, chain, include_block_number=True)

    new_autopool_state_rows = []

    def _extract_autopool_state(row: dict):
        for autopool in autopools:
            new_autopool_state_rows.append(
                AutopoolStates(
                    autopool_vault_address=autopool.autopool_eth_addr,
                    block=int(row.get("block")),
                    chain_id=chain.chain_id,
                    total_shares=row.get((autopool.autopool_eth_addr, "total_shares")),
                    total_nav=row.get((autopool.autopool_eth_addr, "total_nav")),
                    nav_per_share=row.get((autopool.autopool_eth_addr, "nav_per_share")),
                )
            )

    autopool_state_df.apply(_extract_autopool_state, axis=1)

    return new_autopool_state_rows


def _fetch_and_insert_new_autopool_states(autopools: list[AutopoolConstants], chain: ChainData) -> None:
    missing_blocks = _determine_what_blocks_are_needed(autopools, chain)
    if not missing_blocks:
        return

    new_autopool_states_rows = _fetch_new_autopool_state_rows(autopools, missing_blocks, chain)

    insert_avoid_conflicts(
        new_autopool_states_rows,
        AutopoolStates,
        index_elements=[AutopoolStates.autopool_vault_address, AutopoolStates.chain_id, AutopoolStates.block],
    )


def _determine_what_blocks_are_needed(autopools: list[AutopoolConstants], chain: ChainData) -> list[int]:
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
        where_clause=(DestinationStates.chain_id == chain.chain_id)
        & (AutopoolDestinations.autopool_vault_address.in_([a.autopool_eth_addr for a in autopools])),
    )["block"].unique()

    blocks_to_fetch = get_subset_not_already_in_column(
        AutopoolStates,
        AutopoolStates.block,
        blocks_expected_to_have,
        where_clause=AutopoolStates.autopool_vault_address.in_([a.autopool_eth_addr for a in autopools]),
    )
    return blocks_to_fetch


def ensure_autopool_states_are_current():

    # I don't like this format, just do it at the autopool level
    for chain in ALL_CHAINS:
        autopools = [a for a in ALL_AUTOPOOLS_DATA_ON_CHAIN if a.chain == chain]
        if autopools:
            _fetch_and_insert_new_autopool_states(autopools, chain)

        # these are different because the blocks are diferent,
        # eg the rebalannce plant blocks don't always line up with the highest block of each day
        autopools = [a for a in ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN if a.chain == chain]
        if autopools:
            _fetch_and_insert_new_autopool_states(autopools, chain)


if __name__ == "__main__":

    from mainnet_launch.constants import *

    profile_function(ensure_autopool_states_are_current)
