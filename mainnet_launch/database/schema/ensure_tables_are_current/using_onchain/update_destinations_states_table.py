import pandas as pd
from multicall import Call


from mainnet_launch.database.schema.full import Autopools, DestinationStates, Destinations, AutopoolDestinations

from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    merge_tables_as_df,
    TableSelector,
)
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
)

from mainnet_launch.data_fetching.block_timestamp import ensure_all_blocks_are_in_table
from mainnet_launch.constants import (
    ChainData,
    ALL_CHAINS,
    POINTS_HOOK,
    ROOT_PRICE_ORACLE,
    USDC,
    WETH,
    ETH_CHAIN,
    BASE_CHAIN,
    AUTO_USD,
    ALL_AUTOPOOLS_DATA_ON_CHAIN,
    AutopoolConstants,
)


def build_lp_token_spot_price_calls(
    destination_addresses: list[str],
    lp_token_addresses: list[str],
    pool_addresses: list[str],
    chain: ChainData,
    base_asset: str,
) -> list[Call]:

    if base_asset in [USDC(ETH_CHAIN), USDC(BASE_CHAIN)]:
        base_asset_decimals = 6
    elif base_asset in [WETH(ETH_CHAIN), WETH(BASE_CHAIN)]:
        base_asset_decimals = 18
    else:
        raise ValueError("Unexpected base_asset", base_asset)

    def _handle_getRangePricesLP(success, args):
        if success:
            spotPriceInQuote, safePriceInQuote, isSpotSafe = args
            lp_token_spot_price = spotPriceInQuote / (10**base_asset_decimals)
            lp_token_safe_price = safePriceInQuote / (10**base_asset_decimals)
            return (lp_token_spot_price, lp_token_safe_price)

    return [
        Call(
            ROOT_PRICE_ORACLE(chain),
            ["getRangePricesLP(address,address,address)((uint256,uint256,uint256))", lp_token, pool, base_asset],
            [((destination, "lp_token_spot_and_safe"), _handle_getRangePricesLP)],
        )
        for destination, lp_token, pool in zip(destination_addresses, lp_token_addresses, pool_addresses)
    ]


def _fetch_lp_token_spot_prices(
    autopool_to_all_ever_active_destinations: dict[str, list[Destinations]],
    missing_blocks: list[int],
    chain: ChainData,
) -> pd.DataFrame:
    autopool_orm: list[Autopools] = get_full_table_as_orm(Autopools, where_clause=Autopools.chain_id == chain.chain_id)
    destination_orm: list[Destinations] = get_full_table_as_orm(
        Destinations, where_clause=Destinations.chain_id == chain.chain_id
    )

    lp_token_spot_prices_calls = []

    for autopool_vault_address in autopool_to_all_ever_active_destinations.keys():
        this_autopool_active_destinations: list[Destinations] = [
            dest
            for dest in destination_orm
            if dest.destination_vault_address in autopool_to_all_ever_active_destinations[autopool_vault_address]
        ]

        base_asset = [a.base_asset for a in autopool_orm if a.autopool_vault_address == autopool_vault_address][0]

        destination_vault_addresses = [dest.destination_vault_address for dest in this_autopool_active_destinations]
        lp_token_addresses = [dest.underlying for dest in this_autopool_active_destinations]
        pool_addresses = [dest.pool for dest in this_autopool_active_destinations]

        calls = build_lp_token_spot_price_calls(
            destination_vault_addresses, lp_token_addresses, pool_addresses, chain, base_asset
        )
        lp_token_spot_prices_calls.extend(calls)

    lp_token_spot_price_df = get_raw_state_by_blocks(
        lp_token_spot_prices_calls, missing_blocks, chain, include_block_number=True
    )
    return lp_token_spot_price_df


# def build_autopool_balance_of_calls_by_destination(
#     autopool_vault_address: str, destination_vault_addresses: list[str]
# ) -> list[Call]:
#     return [
#         Call(
#             destination_vault_address,
#             ["balanceOf(address)(uint256)", autopool_vault_address],
#             [((autopool_vault_address, destination_vault_address, "balanceOf"), safe_normalize_with_bool_success)],
#         )
#         for destination_vault_address in destination_vault_addresses
#     ]


# def fetch_autopool_balance_of_by_destination(
#     autopool_to_all_ever_active_destinations: dict[str, list[Destinations]], missing_blocks: list[int], chain: ChainData
# ) -> pd.DataFrame:
#     autopool_balance_of_calls = []

#     for autopool_vault_address in autopool_to_all_ever_active_destinations.keys():
#         this_autopool_active_destinations = [
#             dest.destination_vault_address for dest in autopool_to_all_ever_active_destinations[autopool_vault_address]
#         ]

#         autopool_balance_of_calls.extend(
#             build_autopool_balance_of_calls_by_destination(autopool_vault_address, this_autopool_active_destinations)
#         )

#     autopool_destination_balance_of_df = get_raw_state_by_blocks(
#         autopool_balance_of_calls, missing_blocks, chain, include_block_number=True
#     )
#     return autopool_destination_balance_of_df


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
            dest for dest in autopool_to_all_ever_active_destinations[autopool_vault_address]
        ]
        all_active_destinations.update(this_autopool_active_destinations)

    calls = build_destinations_underlyingTotalSupply_calls(list(all_active_destinations))
    destination_total_supply_df = get_raw_state_by_blocks(calls, missing_blocks, chain, include_block_number=True)
    return destination_total_supply_df


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
    autopool_to_all_ever_active_destinations: dict[str, list[str]], missing_blocks: list[int], chain: ChainData
) -> pd.DataFrame:
    autopool_points_calls = []

    for autopool_vault_address in autopool_to_all_ever_active_destinations.keys():
        this_autopool_active_destinations = [
            dest for dest in autopool_to_all_ever_active_destinations[autopool_vault_address]
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
    destination_vault_address: str,
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
            destination_vault_address,
            direction_enum,
            amount,
        ],
        [((autopool.autopool_vault_address, destination_vault_address, direction), _clean_summary_stats_info)],
    )


def _fetch_destination_summary_stats_df(
    autopool_to_all_ever_active_destinations: dict, missing_blocks: list[int], chain: ChainData
) -> pd.DataFrame:
    full_autopool_summary_stats_df = None
    # TODO switch to autopool Destinations
    autopools_orm: list[Autopools] = get_full_table_as_orm(Autopools, where_clause=Autopools.chain_id == chain.chain_id)

    for autopool_vault_address, this_autopool_active_destinations in autopool_to_all_ever_active_destinations.items():

        autopool = [a for a in autopools_orm if a.autopool_vault_address == autopool_vault_address][0]
        all_summary_stats_calls = []
        for dest in this_autopool_active_destinations:
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

    return full_autopool_summary_stats_df


def _extract_new_destination_states(
    autopool_summary_stats_df: pd.DataFrame,
    destination_underlying_total_supply_df: pd.DataFrame,
    autopool_points_df: pd.DataFrame,
    lp_token_spot_price_df: pd.DataFrame,
    autopool_to_all_ever_active_destinations: dict[str | list[Destinations]],
    chain: ChainData,
):
    all_new_destination_states = []
    # autopool_summary_stats_df, destination_underlying_total_supply_df, token_value_df, autopool_to_all_ever_active_destinations
    raw_destination_states_df = pd.merge(autopool_summary_stats_df, destination_underlying_total_supply_df, on="block")
    raw_destination_states_df = pd.merge(raw_destination_states_df, autopool_points_df, on="block")
    raw_destination_states_df = pd.merge(raw_destination_states_df, lp_token_spot_price_df, on="block")

    for autopool_vault_address in autopool_to_all_ever_active_destinations.keys():
        for destination_vault_address in autopool_to_all_ever_active_destinations[autopool_vault_address]:
            destination_vault_address: str

            def _extract_destination_states(row: pd.DataFrame) -> None:
                in_summary_stats = row.get((autopool_vault_address, destination_vault_address, "in"), {}) or {}
                out_summary_stats = row.get((autopool_vault_address, destination_vault_address, "out"), {}) or {}

                total_apr_in = in_summary_stats.get("compositeReturn")
                total_apr_out = out_summary_stats.get("compositeReturn")

                incentive_apr = in_summary_stats.get("incentiveApr")
                fee_apr = in_summary_stats.get("feeApr")
                base_apr = in_summary_stats.get("baseApr")

                safe_total_supply = in_summary_stats.get("safeTotalSupply")

                points_apr = row[(destination_vault_address, "points")]
                underlying_total_supply = row[(destination_vault_address, "underlyingTotalSupply")]
                possible_safe_and_spot_price = row[(destination_vault_address, "lp_token_spot_and_safe")]
                if possible_safe_and_spot_price is None:
                    lp_token_spot_price = None
                    lp_token_safe_price = None
                else:
                    lp_token_spot_price, lp_token_safe_price = possible_safe_and_spot_price

                new_destination_state = DestinationStates(
                    destination_vault_address=destination_vault_address,
                    block=int(row["block"]),
                    chain_id=chain.chain_id,
                    incentive_apr=incentive_apr,
                    fee_apr=fee_apr,
                    base_apr=base_apr,
                    points_apr=points_apr,
                    fee_plus_base_apr=None,
                    total_apr_in=total_apr_in,
                    total_apr_out=total_apr_out,
                    underlying_token_total_supply=underlying_total_supply,
                    safe_total_supply=safe_total_supply,
                    lp_token_spot_price=lp_token_spot_price,
                    lp_token_safe_price=lp_token_safe_price,
                    from_rebalance_plan=False,
                )
                all_new_destination_states.append(new_destination_state)

            raw_destination_states_df.apply(_extract_destination_states, axis=1)

    return all_new_destination_states


def _add_new_destination_states_to_db(possible_blocks: list[int], chain: ChainData):
    missing_blocks = get_subset_not_already_in_column(  # consider switching to looking at timestamps instead
        DestinationStates,
        DestinationStates.block,
        possible_blocks,
        where_clause=DestinationStates.chain_id == chain.chain_id,
    )
    if not missing_blocks:
        return

    ensure_all_blocks_are_in_table(missing_blocks, chain)

    autopool_and_destinations_df = merge_tables_as_df(
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
                Destinations.underlying_name,
                join_on=AutopoolDestinations.destination_vault_address == Destinations.destination_vault_address,
            ),
        ],
        where_clause=(Destinations.chain_id == chain.chain_id)
        & (AutopoolDestinations.autopool_vault_address.in_([a.autopool_eth_addr for a in ALL_AUTOPOOLS_DATA_ON_CHAIN])),
    )

    autopool_to_all_ever_active_destinations = (
        autopool_and_destinations_df.groupby("autopool_vault_address")["destination_vault_address"]
        .apply(tuple)
        .to_dict()
    )
    # for k, v in autopool_to_all_ever_active_destinations.items():
    #     print(k, len(v))

    destination_underlying_total_supply_df = _fetch_destination_total_supply_df(
        autopool_to_all_ever_active_destinations, missing_blocks, chain
    )

    autopool_points_df = _fetch_autopool_points_apr(autopool_to_all_ever_active_destinations, missing_blocks, chain)

    lp_token_spot_price_df = _fetch_lp_token_spot_prices(
        autopool_to_all_ever_active_destinations, missing_blocks, chain
    )

    autopool_summary_stats_df = _fetch_destination_summary_stats_df(
        autopool_to_all_ever_active_destinations, missing_blocks, chain
    )

    all_new_destination_states = _extract_new_destination_states(
        autopool_summary_stats_df,
        destination_underlying_total_supply_df,
        autopool_points_df,
        lp_token_spot_price_df,
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


def _fetch_idle_destination_states(
    chain: ChainData, autopools: list[AutopoolConstants], missing_blocks: list[int]
) -> list[DestinationStates]:
    idle_destination_states = []
    for autopool in autopools:
        for block in missing_blocks:
            idle_destination_states.append(
                DestinationStates(
                    destination_vault_address=autopool.autopool_eth_addr,
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
                    lp_token_spot_price=1.0,
                    lp_token_safe_price=1.0,
                    from_rebalance_plan=False,
                )
            )
    return idle_destination_states


def ensure_destination_states_are_current():
    # only from onchain
    for chain in ALL_CHAINS:
        possible_blocks = build_blocks_to_use(chain)  # the highest block of each full day on this chain
        _add_new_destination_states_to_db(possible_blocks, chain)


import cProfile, pstats

if __name__ == "__main__":
    profiler = cProfile.Profile()
    profiler.enable()
    ensure_destination_states_are_current()
    profiler.disable()
    profiler.dump_stats(
        "mainnet_launch/database/schema/ensure_tables_are_current/ensure_destination_states_are_current.prof"
    )
    stats = pstats.Stats(
        "mainnet_launch/database/schema/ensure_tables_are_current/ensure_destination_states_are_current.prof"
    )
    stats.strip_dirs().sort_stats("cumtime").print_stats(30)
