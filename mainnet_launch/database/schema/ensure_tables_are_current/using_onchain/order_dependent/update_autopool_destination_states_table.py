from multicall import Call

from mainnet_launch.database.schema.full import (
    AutopoolDestinationStates,
    DestinationStates,
    AutopoolDestinations,
    Destinations,
)
from mainnet_launch.database.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    merge_tables_as_df,
    TableSelector,
    _exec_sql_and_cache,
)
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    safe_normalize_6_with_bool_success,
)
from mainnet_launch.constants import (
    ALL_AUTOPOOLS,
    AutopoolConstants,
)


def _determine_what_blocks_are_needed(autopool: AutopoolConstants) -> list[int]:
    auto_addr = autopool.autopool_eth_addr
    chain_id = autopool.chain.chain_id  # keep chain scoping explicit

    sql = f"""
        SELECT DISTINCT ds.block
        FROM autopool_destinations AS ad
        JOIN destination_states AS ds
          ON ds.destination_vault_address = ad.destination_vault_address
         AND ds.chain_id = ad.chain_id
        WHERE ad.autopool_vault_address = '{auto_addr}'
          AND ds.chain_id = {int(chain_id)}
          AND NOT EXISTS (
              SELECT 1
              FROM autopool_destination_states AS ads
              WHERE ads.autopool_vault_address = ad.autopool_vault_address
                AND ads.chain_id = ds.chain_id
                AND ads.block = ds.block
          )
        ORDER BY ds.block;
    """

    df = _exec_sql_and_cache(sql)
    return [] if df.empty else df["block"].astype(int).tolist()


def _fetch_and_insert_new_autopool_destination_states(autopool: AutopoolConstants):
    missing_blocks = _determine_what_blocks_are_needed(autopool)
    if len(missing_blocks) == 0:
        return

    destination_info_df = merge_tables_as_df(  # TODO, use the view instead
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
            owned_shares = row[destination_vault_address]
            if owned_shares is None:
                owned_shares = 0
            else:
                owned_shares = float(owned_shares)

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

    from mainnet_launch.constants import *

    profile_function(ensure_autopool_destination_states_are_current)
    # profile_function(_fetch_and_insert_new_autopool_destination_states, BASE_USD)  # 3,3 seconds

# _determine_what_blocks_are_needed(ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN, ALL_CHAINS[0])
