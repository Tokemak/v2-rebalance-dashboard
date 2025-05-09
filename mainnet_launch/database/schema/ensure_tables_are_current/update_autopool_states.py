import pandas as pd
from multicall import Call

from mainnet_launch.database.schema.full import Autopools, AutopoolStates, AutopoolDestinationStates, DestinationStates

from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
    get_subset_not_already_in_column,
    natural_left_right_using_where,
    insert_avoid_conflicts,
    merge_tables_as_df,
    TableSelector,
)

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    safe_normalize_6_with_bool_success,
    build_blocks_to_use,
)

from mainnet_launch.constants import ALL_CHAINS, ChainData, USDC, WETH


def _fetch_new_autopool_state_rows(
    autopools: list[Autopools], missing_blocks: list[int], chain: ChainData
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
        if autopool.base_asset in USDC:
            total_nav_cleaning_function = _extract_debt_plus_idle_6
            nav_per_share_cleaning_function = safe_normalize_6_with_bool_success
        elif autopool.base_asset in WETH:
            total_nav_cleaning_function = _extract_debt_plus_idle_18
            nav_per_share_cleaning_function = safe_normalize_with_bool_success
        else:
            raise ValueError(f"Unknown base asset {autopool.base_asset} for autopool {autopool.autopool_vault_address}")

        calls.extend(
            [
                Call(
                    autopool.autopool_vault_address,
                    ["totalSupply()(uint256)"],
                    [
                        ((autopool.autopool_vault_address, "total_shares"), safe_normalize_with_bool_success)
                    ],  # always 1e18
                ),
                Call(
                    autopool.autopool_vault_address,
                    ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
                    [((autopool.autopool_vault_address, "total_nav"), total_nav_cleaning_function)],
                ),
                Call(
                    autopool.autopool_vault_address,
                    ["convertToAssets(uint256)(uint256)", int(1e18)],
                    [((autopool.autopool_vault_address, "nav_per_share"), nav_per_share_cleaning_function)],
                ),
            ]
        )

    autopool_state_df = get_raw_state_by_blocks(calls, missing_blocks, chain, include_block_number=True)

    new_autopool_state_rows = []

    def _extract_autopool_state(row: dict):
        for autopool in autopools:
            new_autopool_state_rows.append(
                AutopoolStates(
                    autopool_vault_address=autopool.autopool_vault_address,
                    block=int(row["block"]),
                    chain_id=chain.chain_id,
                    total_shares=float(row[(autopool.autopool_vault_address, "total_shares")]),
                    total_nav=float(row[(autopool.autopool_vault_address, "total_nav")]),
                    nav_per_share=float(row[(autopool.autopool_vault_address, "nav_per_share")]),
                )
            )

    autopool_state_df.apply(_extract_autopool_state, axis=1)

    return new_autopool_state_rows


def _add_new_autopool_states_to_db(possible_blocks: list[int], chain: ChainData) -> None:
    missing_blocks = get_subset_not_already_in_column(
        AutopoolStates,
        AutopoolStates.block,
        possible_blocks,
        where_clause=AutopoolStates.chain_id == chain.chain_id,
    )

    if len(missing_blocks) == 0:
        return

    autopools = get_full_table_as_orm(Autopools, where_clause=Autopools.chain_id == chain.chain_id)
    new_autopool_states_rows = _fetch_new_autopool_state_rows(autopools, missing_blocks, chain)

    insert_avoid_conflicts(
        new_autopool_states_rows,
        AutopoolStates,
        index_elements=[AutopoolStates.autopool_vault_address, AutopoolStates.chain_id, AutopoolStates.block],
    )


def ensure_autopool_states_are_current():
    for chain in ALL_CHAINS:
        needed_blocks = merge_tables_as_df(
            [
                TableSelector(
                    DestinationStates,
                    DestinationStates.block,
                )
            ],
            where_clause=DestinationStates.chain_id == chain.chain_id,
        )["block"].tolist()
        _add_new_autopool_states_to_db(needed_blocks, chain)


if __name__ == "__main__":
    ensure_autopool_states_are_current()
