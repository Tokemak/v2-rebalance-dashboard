import pandas as pd
from multicall import Call


from mainnet_launch.database.schema.full import (
    DestinationTokenValues,
    TokenValues,
    Autopools,
    DestinationStates,
    DestinationTokens,
    Destinations,
    Tokens,
)

from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
    get_full_table_as_df,
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
from mainnet_launch.constants import ChainData, ALL_CHAINS, ALL_AUTOPOOLS, POINTS_HOOK

from mainnet_launch.pages.autopool_diagnostics.lens_contract import (
    fetch_autopool_to_active_destinations_over_this_period_of_missing_blocks,
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


def build_destinations_underlyingTotalSupply_calls(destination_vault_addresses: list[str]) -> list[Call]:
    return [
        Call(
            destination_vault_address,
            ["underlyingTotalSupply()(uint256)"],
            [((destination_vault_address, "underlyingTotalSupply"), safe_normalize_with_bool_success)],
        )
        for destination_vault_address in destination_vault_addresses
    ]


def _fetch_destination_total_supply_df(
    autopool_to_all_ever_active_destinations: dict, missing_blocks: list[int], chain: ChainData
) -> pd.DataFrame:
    all_active_destinations = set()

    for autopool_vault_address in autopool_to_all_ever_active_destinations.keys():
        this_autopool_active_destinations = [
            dest.destination_vault_address for dest in autopool_to_all_ever_active_destinations[autopool_vault_address]
        ]
        all_active_destinations.update(this_autopool_active_destinations)

    calls = build_destinations_underlyingTotalSupply_calls(list(all_active_destinations))
    destination_total_supply_df = get_raw_state_by_blocks(calls, missing_blocks, chain, include_block_number=True)
    # looks right
    return destination_total_supply_df


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
    return autopool_destination_balance_of_df


def _build_destination_points_calls(this_autopool_active_destinations: list[str], chain: ChainData) -> list[Call]:
    return [
        Call(
            POINTS_HOOK(chain),
            ["destinationBoosts(address)(uint256)", destination_vault_address],
            [((destination_vault_address, "points"), safe_normalize_with_bool_success)],
        )
        for destination_vault_address in this_autopool_active_destinations
    ]


def _fetch_autopool_points_apr(
    autopool_to_all_ever_active_destinations: dict[str, list[Destinations]], missing_blocks: list[int], chain: ChainData
) -> pd.DataFrame:
    autopool_points_calls = []

    for autopool_vault_address in autopool_to_all_ever_active_destinations.keys():
        this_autopool_active_destinations = [
            dest.destination_vault_address for dest in autopool_to_all_ever_active_destinations[autopool_vault_address]
        ]

        autopool_points_calls.extend(_build_destination_points_calls(this_autopool_active_destinations, chain))

    autopool_points_df = get_raw_state_by_blocks(
        autopool_points_calls, missing_blocks, chain, include_block_number=True
    )
    return autopool_points_df


def _clean_summary_stats_info(success, summary_stats):
    if success is True:
        summary = {
            "destination": summary_stats[0],  # address
            "baseApr": summary_stats[1] / 1e18,
            "feeApr": summary_stats[2] / 1e18,
            "incentiveApr": summary_stats[3] / 1e18,
            "safeTotalSupply": summary_stats[4] / 1e18,
            "priceReturn": summary_stats[5] / 1e18,
            "maxDiscount": summary_stats[6] / 1e18,
            "maxPremium": summary_stats[7] / 1e18,
            "ownedShares": summary_stats[8] / 1e18,
            "compositeReturn": summary_stats[9] / 1e18,
            "pricePerShare": summary_stats[10] / 1e18,
            "pointsApr": None,  # set later
        }
        return summary
    else:
        return None


def _build_summary_stats_call(
    autopool: Autopools,
    dest: Destinations,
    direction: str = "out",
    amount: int = 0,
) -> Call:
    # /// @notice Gets the safe price of the underlying LP token
    # /// @dev Price validated to be inside our tolerance against spot price. Will revert if outside.
    # /// @return price Value of 1 unit of the underlying LP token in terms of the base asset
    # function getValidatedSafePrice() external returns (uint256 price);
    # getDestinationSummaryStats uses getValidatedSafePrice. So when prices are outside tolerance this function reverts

    # consider finding a version of this function that won't revert, (follow up, I am pretty sure that does not exist)
    if direction == "in":
        direction_enum = 0
    elif direction == "out":
        direction_enum = 1
    return_types = "(address,uint256,uint256,uint256,uint256,int256,int256,int256,uint256,int256,uint256)"

    # cleaning_function = build_summary_stats_cleaning_function(autopool)
    return Call(
        autopool.strategy_address,
        [
            f"getDestinationSummaryStats(address,uint8,uint256)({return_types})",
            dest.destination_vault_address,
            direction_enum,
            amount,
        ],
        [((autopool.autopool_vault_address, dest.destination_vault_address, direction), _clean_summary_stats_info)],
    )


def _fetch_destination_summary_stats_df(
    autopool_to_all_ever_active_destinations: dict, missing_blocks: list[int], chain: ChainData
) -> pd.DataFrame:
    autopools_orm: list[Autopools] = get_full_table_as_orm(Autopools, where_clause=Autopools.chain_id == chain.chain_id)
    full_autopool_summary_stats_df = None

    for autopool in autopools_orm:
        all_summary_stats_calls = []
        this_autopool_destinations = autopool_to_all_ever_active_destinations[autopool.autopool_vault_address]
        for dest in this_autopool_destinations:
            all_summary_stats_calls.append(_build_summary_stats_call(autopool, dest, "out"))
            all_summary_stats_calls.append(_build_summary_stats_call(autopool, dest, "in"))

        autopool_summary_stats_df = get_raw_state_by_blocks(
            all_summary_stats_calls, missing_blocks, chain, include_block_number=True
        )

        if full_autopool_summary_stats_df is None:
            full_autopool_summary_stats_df = autopool_summary_stats_df.copy()
        else:
            full_autopool_summary_stats_df = pd.merge(
                full_autopool_summary_stats_df, autopool_summary_stats_df, on="block"
            )

    # I think the issue here is that it it getting the None version when it should get the active version
    return full_autopool_summary_stats_df


def _extract_new_destination_states(
    autopool_summary_stats_df: pd.DataFrame,
    destination_underlying_total_supply_df: pd.DataFrame,
    autopool_points_df: pd.DataFrame,
    autopool_to_all_ever_active_destinations: dict[str | list[Destinations]],
    chain: ChainData,
):
    all_new_destination_states = []
    # autopool_summary_stats_df, destination_underlying_total_supply_df, token_value_df, autopool_to_all_ever_active_destinations
    raw_destination_states_df = pd.merge(autopool_summary_stats_df, destination_underlying_total_supply_df, on="block")
    raw_destination_states_df = pd.merge(raw_destination_states_df, autopool_points_df, on="block")

    for autopool_vault_address in autopool_to_all_ever_active_destinations.keys():
        for dest in autopool_to_all_ever_active_destinations[autopool_vault_address]:

            def _extract_destination_states(row: pd.DataFrame) -> None:
                in_summary_stats = row.get((autopool_vault_address, dest.destination_vault_address, "in"), {}) or {}
                out_summary_stats = row.get((autopool_vault_address, dest.destination_vault_address, "out"), {}) or {}

                total_apr_in = in_summary_stats.get("compositeReturn")
                total_apr_out = out_summary_stats.get("compositeReturn")

                incentive_apr = in_summary_stats.get("incentiveApr")
                fee_apr = in_summary_stats.get("feeApr")
                base_apr = in_summary_stats.get("baseApr")

                price_per_share = in_summary_stats.get("pricePerShare")
                price_return = in_summary_stats.get("priceReturn")
                fee_plus_base_apr = None  # only for post autoUSD destinations
                safe_total_supply = in_summary_stats.get("safeTotalSupply")

                points_apr = row[(dest.destination_vault_address, "points")]
                underlying_total_supply = row[(dest.destination_vault_address, "underlyingTotalSupply")]

                new_destination_state = DestinationStates(
                    destination_vault_address=dest.destination_vault_address,
                    block=row["block"],
                    chain_id=chain.chain_id,
                    incentive_apr=incentive_apr,
                    fee_apr=fee_apr,
                    base_apr=base_apr,
                    points_apr=points_apr,
                    fee_plus_base_apr=fee_plus_base_apr,
                    total_apr_in=total_apr_in,
                    total_apr_out=total_apr_out,
                    underlying_token_total_supply=underlying_total_supply,
                    safe_total_supply=safe_total_supply,
                    price_per_share=price_per_share,
                    price_return=price_return,
                )
                all_new_destination_states.append(new_destination_state)

            raw_destination_states_df.apply(_extract_destination_states, axis=1)

    return all_new_destination_states


def ensure_destination_states_are_current():
    for chain in ALL_CHAINS:
        possible_blocks = build_blocks_to_use(chain)

        missing_blocks = get_subset_not_already_in_column(
            DestinationStates,
            DestinationStates.block,
            possible_blocks,
            where_clause=DestinationStates.chain_id == chain.chain_id,
        )
        print(len(missing_blocks))
        if len(missing_blocks) == 0:
            continue

        # token_value_df = merge_tables_as_df(
        #     [
        #         TableSelector(
        #             table=DestinationTokenValues,
        #         ),
        #         TableSelector(
        #             table=TokenValues,
        #             join_on=(
        #                 (DestinationTokenValues.block == TokenValues.block)
        #                 & (DestinationTokenValues.chain_id == TokenValues.chain_id)
        #                 & (DestinationTokenValues.token_address == TokenValues.token_address)
        #             ),
        #         ),
        #         TableSelector(
        #             table=Tokens,
        #             select_fields=[Tokens.symbol, Tokens.decimals, Tokens.token_address],
        #             join_on=(
        #                 (DestinationTokenValues.chain_id == Tokens.chain_id)
        #                 & (DestinationTokenValues.token_address == Tokens.token_address)
        #             ),
        #         ),
        #     ],
        #     where_clause=(DestinationTokenValues.chain_id == chain.chain_id),
        # )
        autopool_to_all_ever_active_destinations = (
            fetch_autopool_to_active_destinations_over_this_period_of_missing_blocks(chain, missing_blocks)
        )

        destination_underlying_total_supply_df = _fetch_destination_total_supply_df(
            autopool_to_all_ever_active_destinations, missing_blocks, chain
        )

        autopool_points_df = _fetch_autopool_points_apr(autopool_to_all_ever_active_destinations, missing_blocks, chain)

        autopool_summary_stats_df = _fetch_destination_summary_stats_df(
            autopool_to_all_ever_active_destinations, missing_blocks, chain
        )

        all_new_destination_states = _extract_new_destination_states(
            autopool_summary_stats_df,
            destination_underlying_total_supply_df,
            autopool_points_df,
            autopool_to_all_ever_active_destinations,
            chain,
        )

        idle_destination_states = _fetch_idle_destination_states(chain, missing_blocks)

        insert_avoid_conflicts(
            [
                *all_new_destination_states,
                *idle_destination_states,
            ],
            DestinationStates,
            index_elements=[
                DestinationStates.block,
                DestinationStates.chain_id,
                DestinationStates.destination_vault_address,
            ],
        )


def _fetch_idle_destination_states(chain: ChainData, missing_blocks: list[int]) -> list[DestinationStates]:

    autopools_as_destinations: list[Destinations] = get_full_table_as_orm(
        Destinations, where_clause=(Destinations.chain_id == chain.chain_id) & (Destinations.pool_type == "idle")
    )

    idle_destination_states = []
    for dest in autopools_as_destinations:
        for block in missing_blocks:
            idle_destination_states.append(
                DestinationStates(
                    destination_vault_address=dest.destination_vault_address,
                    block=block,
                    chain_id=chain.chain_id,
                    incentive_apr=0.0,
                    fee_apr=0.0,
                    base_apr=0.0,
                    points_apr=0.0,
                    fee_plus_base_apr=None,
                    total_apr_in=0.0,
                    total_apr_out=0.0,
                    underlying_token_total_supply=None,
                    safe_total_supply=None,
                    price_per_share=1.0,
                    price_return=0.0,
                )
            )
    return idle_destination_states


if __name__ == "__main__":
    ensure_destination_states_are_current()
