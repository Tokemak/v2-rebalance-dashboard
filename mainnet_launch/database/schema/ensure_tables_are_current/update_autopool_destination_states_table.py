from multicall import Call

from mainnet_launch.pages.autopool_diagnostics.lens_contract import (
    fetch_autopool_to_active_destinations_over_this_period_of_missing_blocks,
)

from mainnet_launch.database.schema.full import (
    DestinationTokenValues,
    Destinations,
    AutopoolDestinationStates,
    DestinationStates,
)
from mainnet_launch.database.schema.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    natural_left_right_using_where,
    merge_tables_as_df,
    TableSelector,
)
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
)
from mainnet_launch.constants import ALL_CHAINS, ChainData, ALL_AUTOPOOLS


def build_autopool_balance_of_calls_by_destination(
    autopool_vault_address: str, destination_vault_addresses: list[str]
) -> list[Call]:
    "How many lp tokens of the destination does the autopool own?"
    return [
        Call(
            destination_vault_address,
            ["balanceOf(address)(uint256)", autopool_vault_address],
            [((autopool_vault_address, destination_vault_address, "balanceOf"), safe_normalize_with_bool_success)],
        )
        for destination_vault_address in destination_vault_addresses
    ]


def fetch_autopool_balance_of_by_destination(
    missing_blocks: list[int], chain: ChainData
) -> list[AutopoolDestinationStates]:

    autopool_to_all_ever_active_destinations = fetch_autopool_to_active_destinations_over_this_period_of_missing_blocks(
        chain, missing_blocks
    )

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

    new_autopool_destination_states_rows = []

    def _extract_autopool_destination_vault_balance_of_block(row: dict):
        for k in row.keys():
            if k != "block":
                autopool_vault_address, destination_vault_address, _ = k
                quantity = row[k]
                new_autopool_destination_states_rows.append(
                    AutopoolDestinationStates(
                        destination_vault_address=destination_vault_address,
                        autopool_vault_address=autopool_vault_address,
                        block=int(row["block"]),
                        chain_id=chain.chain_id,
                        owned_shares=float(quantity),
                    )
                )

    autopool_destination_balance_of_df.apply(_extract_autopool_destination_vault_balance_of_block, axis=1)

    return new_autopool_destination_states_rows


def ensure_autopool_destination_states_are_current():
    for autopool in ALL_AUTOPOOLS:

        needed_blocks = merge_tables_as_df(
            [
                TableSelector(
                    DestinationStates,
                    DestinationStates.block,
                )
            ],
            where_clause=DestinationStates.chain_id == autopool.chain.autopool.chain_id,
        )["block"].tolist()

        # somehow I just want ot get

        needed_blocks = list(set(needed_blocks))

        missing_blocks = get_subset_not_already_in_column(
            AutopoolDestinationStates,
            AutopoolDestinationStates.block,
            needed_blocks,
            where_clause=AutopoolDestinationStates.chain_id == chain.chain_id,
        )

        if len(missing_blocks) == 0:
            continue

        new_autopool_destination_state_rows = fetch_autopool_balance_of_by_destination(missing_blocks, chain)

        idle_autopool_destination_states = _build_idle_autopool_destination_states(chain, missing_blocks)

        insert_avoid_conflicts(
            [*new_autopool_destination_state_rows, *idle_autopool_destination_states],
            AutopoolDestinationStates,
            index_elements=[
                AutopoolDestinationStates.destination_vault_address,
                AutopoolDestinationStates.autopool_vault_address,
                AutopoolDestinationStates.block,
                AutopoolDestinationStates.chain_id,
            ],
        )


def _build_idle_autopool_destination_states(
    chain: ChainData, missing_blocks: list[int]
) -> list[AutopoolDestinationStates]:

    idle_destination_token_values_df = natural_left_right_using_where(
        Destinations,
        DestinationTokenValues,
        using=[Destinations.destination_vault_address, Destinations.chain_id],
        where_clause=(
            (DestinationTokenValues.chain_id == chain.chain_id)
            & DestinationTokenValues.block.in_(missing_blocks)
            & (Destinations.pool_type == "idle")
        ),
    )

    idle_autopool_destination_states = []

    def _extract_idle_autopool_destination_state(row: dict):
        idle_autopool_destination_states.append(
            AutopoolDestinationStates(
                destination_vault_address=row["destination_vault_address"],
                autopool_vault_address=row["destination_vault_address"],
                block=int(row["block"]),
                chain_id=chain.chain_id,
                owned_shares=float(row["quantity"]),
            )
        )

    idle_destination_token_values_df.apply(_extract_idle_autopool_destination_state, axis=1)

    return idle_autopool_destination_states


if __name__ == "__main__":
    ensure_autopool_destination_states_are_current()


# TODO add idle here
# view from destination token values
# def _fetch_actual_nav_per_share_by_day(autopool: AutopoolConstants, blocks: list[int]) -> pd.DataFrame:
#     def handle_getAssetBreakdown(success, AssetBreakdown):
#         if success:
#             totalIdle, totalDebt, totalDebtMin, totalDebtMax = AssetBreakdown
#             return int(totalIdle + totalDebt) / 1e18
#         return None

#     calls = [
#         Call(
#             autopool.autopool_eth_addr,
#             ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
#             [("actual_nav", handle_getAssetBreakdown)],
#         ),
#         Call(
#             autopool.autopool_eth_addr,
#             ["totalSupply()(uint256)"],
#             [("actual_shares", safe_normalize_with_bool_success)],
#         ),
#     ]

#     df = get_raw_state_by_blocks(calls, blocks, autopool.chain, include_block_number=True).reset_index()
#     df["autopool"] = autopool.name
#     return df


# def _extract_new_autopool_destination_state_rows(
#     destination_states_df: pd.DataFrame, autopool_destination_balance_of_df: pd.DataFrame, chain: ChainData
# ):
#     limited_destination_states_df = destination_states_df[
#         [
#             "destination_vault_address",
#             "block",
#             "underlying_token_total_supply",
#             "underlying_safe_price",
#             "underlying_spot_price",
#             "underlying_backing",
#         ]
#     ].copy()
#     raw_autopool_destination_state_df = pd.merge(
#         limited_destination_states_df, autopool_destination_balance_of_df, on=["block", "destination_vault_address"]
#     )

#     new_autopool_destination_state_rows = []

#     def _extract_autopool_destination_state(row: dict) -> None:
#         new_autopool_destination_state_rows.append(
#             AutopoolDestinationStates(
#                 destination_vault_address=row["destination_vault_address"],
#                 autopool_vault_address=row["autopool_vault_address"],
#                 block=row["block"],
#                 chain_id=chain.chain_id,
#                 quantity=row["balance_of"],
#                 total_safe_value=row["balance_of"] * row["underlying_safe_price"],
#                 total_spot_value=row["balance_of"] * row["underlying_spot_price"],
#                 total_backing_value=row["balance_of"] * row["underlying_backing"],
#                 percent_ownership=100 * (row["balance_of"] / row["underlying_token_total_supply"]),
#             )
#         )

#     raw_autopool_destination_state_df.apply(_extract_autopool_destination_state, axis=1)

#     return new_autopool_destination_state_rows
