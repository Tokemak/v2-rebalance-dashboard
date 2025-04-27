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
    get_state_by_one_block,
)
from mainnet_launch.pages.autopool_diagnostics.lens_contract import (
    fetch_active_destinations_by_autopool_by_block,
    fetch_pools_and_destinations_df,
)
from mainnet_launch.constants import (
    AutopoolConstants,
    ALL_AUTOPOOLS,
    AUTO_LRT,
    POINTS_HOOK,
    ChainData,
)

raise ValueError("not done")


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
        [((autopool.vault_address, dest.destination_vault_address), _clean_summary_stats_info)],
    )


def _fetch_autopool_to_active_destinations_over_this_period_of_missing_blocks(
    chain: ChainData, missing_blocks: list[int]
):
    all_destinations_orm: list[Destinations] = get_full_table_as_orm(
        Destinations, where_clause=Destinations.chain_id == chain.chain_id
    )
    all_autopools_orm: list[Autopools] = get_full_table_as_orm(
        Autopools, where_clause=Autopools.chain_id == chain.chain_id
    )

    raw_df = fetch_active_destinations_by_autopool_by_block(chain, missing_blocks)

    active_destinations_by_autopool_df = pd.DataFrame.from_records(raw_df["getPoolsAndDestinations"].values)
    # make a bunch of summary stats calls
    # split up by autopools to avoid max gas costs
    autopool_to_all_ever_active_destinations: dict[str | list[Destinations]] = {}
    for autopool in all_autopools_orm:
        this_autopool_destinations = set()
        all_ever_active_destinations = active_destinations_by_autopool_df[autopool.vault_address].dropna().values
        for active_destinations_at_this_block in all_ever_active_destinations:
            this_autopool_destinations.update(active_destinations_at_this_block)

        autopool_to_all_ever_active_destinations[autopool.vault_address] = [
            d for d in all_destinations_orm if d.destination_vault_address in this_autopool_destinations
        ]
    return autopool_to_all_ever_active_destinations


def _fetch_destination_summary_stats_from_external_source(chain: ChainData):
    possible_blocks = build_blocks_to_use(chain)
    missing_blocks = get_subset_not_already_in_column(
        DestinationStates,
        DestinationStates.block,
        possible_blocks,
        where_clause=DestinationStates.chain_id == chain.chain_id,
    )

    autopool_to_all_ever_active_destinations: dict[str | list[Destinations]] = (
        _fetch_autopool_to_active_destinations_over_this_period_of_missing_blocks(chain, missing_blocks)
    )

    new_rows = _fetch_autopool_and_destination_states(chain, missing_blocks, autopool_to_all_ever_active_destinations)


def _fetch_autopool_and_destination_states(
    chain: ChainData, missing_blocks: list[int], autopool_to_all_ever_active_destinations
):
    # DestinationStates # DestinationTokenValues #TokenValues ( I think I can get get this from the safe and spot price oracle)
    pass


if __name__ == "__main__":
    from mainnet_launch.constants import ETH_CHAIN

    _fetch_destination_summary_stats_from_external_source(ETH_CHAIN)

# def fetch_destination_summary_stats(autopool: AutopoolConstants, summary_stats_field: str):
#     if summary_stats_field not in SUMMARY_STATS_FIELDS:
#         raise ValueError(f"Can only fetch {SUMMARY_STATS_FIELDS=} you tried to fetch {summary_stats_field=}")
# TODO


def _fetch_destination_summary_stats_from_external_source2(chain: ChainData):

    # step one,
    # assumes blocks is constant
    possible_blocks = build_blocks_to_use(chain)

    missing_blocks = get_subset_not_already_in_column(
        DestinationStates,
        DestinationStates.block,
        possible_blocks,
        where_clause=DestinationStates.chain_id == chain.chain_id,
    )

    all_destinations_orm = get_full_table_as_orm(Destinations, where_clause=Destinations.chain_id == chain.chain_id)
    all_autopools_orm = get_full_table_as_orm(Autopools, where_clause=Autopools.chain_id == chain.chain_id)

    autopool_and_vault_by_block = fetch_pools_and_destinations_df(chain, missing_blocks)
    pass

    # approach 1, get all combinations of (dest, autopool) handle the extra downstream.
    # i'm not sure that is right, I think i should jsut fetch what I need
    # calls = []

    # for a in all_autopools_orm:
    #     for dest in all_destinations_orm:
    #         calls.append(_build_summary_stats_call(a, dest))

    # state = get_state_by_one_block(calls, max(missing_blocks), chain)
    # new_destination_states_rows = []
    # new_autopool_states_rows = []

    # for (autopool_vault_address, destination_vault_address), summary_stats_response in state.items():
    #     if summary_stats_response is not None:
    #         DestinationStates(
    #             destination_vault_address=destination_vault_address, block=max(missing_blocks),
    #             chain_id=chain.chain_id,
    #             incentive_apr=
    #         )

    #     if response is not None:
    #         single_destination_summary_stats = destination_states.get(destination_vault_address)
    #         if single_destination_summary_stats is not None:
    #             for k, v in single_destination_summary_stats.items():
    #                 found = response[k]
    #                 if v != found:
    #                     pass
    #                     raise ValueError('expected the same destination to have the same state ')
    #         else:
    #             destination_states[destination_vault_address] = response

    return destination_states
