import pandas as pd
from multicall import Call


from mainnet_launch.database.schema.full import (
    DestinationTokenValues,
    DestinationTokens,
    Destinations,
    Tokens,
    DestinationStates,
    Autopools,
    AutopoolDestinations,
)


from mainnet_launch.database.schema.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    natural_left_right_using_where,
    get_full_table_as_orm,
    TableSelector,
    merge_tables_as_df,
    get_full_table_as_df,
)

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
    identity_with_bool_success,
    safe_normalize_6_with_bool_success,
    get_state_by_one_block,
)

from mainnet_launch.data_fetching.block_timestamp import ensure_all_blocks_are_in_table

from mainnet_launch.constants import (
    ALL_CHAINS,
    ALL_BASE_ASSETS,
    ROOT_PRICE_ORACLE,
    ALL_AUTOPOOLS,
    ChainData,
    AUTO_USD,
    ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN,
    ALL_AUTOPOOLS_DATA_ON_CHAIN,
    AutopoolConstants,
    WETH,
    USDC,
    time_decorator,
    TokemakAddress,
)


SOLVER_ROOT_ORACLE = TokemakAddress(
    eth="0xdB8747a396D75D576Dc7a10bb6c8F02F4a3C20f1",
    base="0x67D29b2d1b422922406d6d5fb7846aE99c282de1",
    sonic="0x4137b35266A4f42ad8B4ae21F14D0289861cc970",
)


def _build_get_spot_price_in_quote_calls(chain: ChainData, destination_address_info_df: pd.DataFrame) -> list[Call]:
    # pricer_contract.functions.getSpotPriceInQuote(underlyingTokens[i], pool, quote).call({}, blockNo)
    # note: this might need to be patched to include autopool.baseAsset -> 1.0
    pool_token_addresses = destination_address_info_df[
        ["pool", "token_address", "base_asset", "base_asset_decimals"]
    ].drop_duplicates()
    calls = []
    for pool_address, token_address, base_asset, base_asset_decimals in zip(
        pool_token_addresses["pool"],
        pool_token_addresses["token_address"],
        pool_token_addresses["base_asset"],
        pool_token_addresses["base_asset_decimals"],
    ):
        if base_asset_decimals == 6:
            cleaning_function = safe_normalize_6_with_bool_success
        elif base_asset_decimals == 18:
            cleaning_function = safe_normalize_with_bool_success
        else:
            raise ValueError("Unexpected Base Asset decimals")

        calls.append(
            Call(
                SOLVER_ROOT_ORACLE(chain),
                ["getSpotPriceInQuote(address,address,address)(uint256)", token_address, pool_address, base_asset],
                [((pool_address, token_address, "spot_price"), cleaning_function)],
            )
        )

    return calls


def _determine_what_blocks_are_still_needed(chain: ChainData, destination_info_df: pd.DataFrame) -> list[int]:
    """
    Determine which blocks are still needed for the destination token values.
    This is done by checking the existing blocks in the DestinationTokenValues table.
    """
    # todo move this up, is 150k rows, only need to fetch it once at the start
    # keep as is for now
    full_destination_token_values_df = get_full_table_as_df(
        DestinationTokenValues,
        where_clause=(DestinationTokenValues.chain_id == chain.chain_id),
    )

    existing_blocks_by_destination = (
        full_destination_token_values_df.groupby("destination_vault_address")["block"].apply(set).to_dict()
    )

    needed_blocks = set()

    for destination_vault_address, blocks_we_already_have in existing_blocks_by_destination.items():
        all_blocks_that_we_neeed = destination_info_df[
            destination_info_df["destination_vault_address"] == destination_vault_address
        ]["block"].unique()

        for block in all_blocks_that_we_neeed:
            if block not in blocks_we_already_have:
                needed_blocks.add(int(block))

    return list(needed_blocks)


@time_decorator
def _fetch_destination_token_value_data_from_external_source(
    chain: ChainData, destination_info_df: pd.DataFrame, needed_blocks: list[int]
) -> pd.DataFrame:

    def build_pool_token_spot_price_calls(
        chain: ChainData, pool_addresses: list[str], token_addresses: list[str]
    ) -> list[Call]:
        return [
            Call(
                ROOT_PRICE_ORACLE(chain),
                ["getSpotPriceInEth(address,address)(uint256)", token_address, pool_address],
                [((pool_address, token_address, "spot_price"), safe_normalize_with_bool_success)],
            )
            for (pool_address, token_address) in zip(pool_addresses, token_addresses)
        ]

    def build_underlying_reserves_calls(destinations: list[str]) -> list[Call]:
        return [
            Call(
                dest,
                "underlyingReserves()(address[],uint256[])",
                [
                    ((dest, "underlyingReserves_tokens"), identity_with_bool_success),
                    ((dest, "underlyingReserves_amounts"), identity_with_bool_success),
                ],
            )
            for dest in destinations
        ]

    unique_destinations = destination_info_df[["pool", "token_address"]].drop_duplicates()

    spot_price_calls = build_pool_token_spot_price_calls(
        chain, unique_destinations["pool"], unique_destinations["token_address"]
    )

    underlying_reserves_calls = build_underlying_reserves_calls(
        destination_info_df["destination_vault_address"].unique()
    )

    spot_df = get_raw_state_by_blocks(
        spot_price_calls,
        needed_blocks,
        chain,
        include_block_number=True,
    )

    reserve_df = get_raw_state_by_blocks(
        underlying_reserves_calls,
        needed_blocks,
        chain,
        include_block_number=False,
    )

    df = spot_df.merge(reserve_df, how="outer", left_index=True, right_index=True)

    return df


def _build_get_spot_price_in_eth_calls(chain: ChainData, destination_address_info_df: pd.DataFrame) -> list[Call]:
    pool_token_addresses = destination_address_info_df[["pool", "token_address"]].drop_duplicates()
    return [
        Call(
            ROOT_PRICE_ORACLE(chain),
            ["getSpotPriceInEth(address,address)(uint256)", token_address, pool_address],
            [((pool_address, token_address, "spot_price"), safe_normalize_with_bool_success)],
        )
        for (pool_address, token_address) in zip(pool_token_addresses["pool"], pool_token_addresses["token_address"])
    ]


def _build_underlying_reserves_calls(destination_address_info_df: pd.DataFrame) -> list[Call]:
    unique_destinations = destination_address_info_df["destination_vault_address"].unique()
    return [
        Call(
            destination_vault_address,
            "underlyingReserves()(address[],uint256[])",
            [
                ((destination_vault_address, "underlyingReserves_tokens"), identity_with_bool_success),
                ((destination_vault_address, "underlyingReserves_amounts"), identity_with_bool_success),
            ],
        )
        for destination_vault_address in unique_destinations
    ]


def _fetch_and_insert_destination_token_values(
    autopools: list[AutopoolConstants],
    chain: ChainData,
):
    # 35k rows
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
                [Destinations.underlying, Destinations.pool],
                join_on=AutopoolDestinations.destination_vault_address == Destinations.destination_vault_address,
            ),
            TableSelector(
                DestinationTokens,
                [DestinationTokens.token_address, DestinationTokens.index],
                join_on=DestinationTokens.destination_vault_address == Destinations.destination_vault_address,
            ),
            TableSelector(
                Tokens,
                [Tokens.decimals],
                join_on=DestinationTokens.token_address == Tokens.token_address,
            ),
            TableSelector(
                DestinationStates,
                DestinationStates.block,
                join_on=(DestinationStates.destination_vault_address == AutopoolDestinations.destination_vault_address),
            ),
        ],
        where_clause=(DestinationStates.chain_id == chain.chain_id)
        & (Destinations.pool_type != "idle")
        & (AutopoolDestinations.autopool_vault_address.in_([a.autopool_eth_addr for a in autopools])),
    )

    needed_blocks = _determine_what_blocks_are_still_needed(chain, destination_info_df)
    if not needed_blocks:
        return
    # needs destination pool, destination lp otken address and destination_vault address
    token_spot_prices_and_reserves_df = _fetch_destination_token_value_data_from_external_source(
        chain, destination_info_df, needed_blocks
    )

    new_destination_token_values_rows = []

    # this is really show, unsure why 90 seconds
    def _extract_destination_token_values(row: dict) -> None:
        token_spot_price_column = (row["pool"], row["token_address"], "spot_price")
        quantity_column = (row["destination_vault_address"], "underlyingReserves_amounts")
        token_address_column = (row["destination_vault_address"], "underlyingReserves_tokens")

        amounts_excluding_pool_token = []  # for composable stable pools
        for quantity_tuple, tokens_tuple in zip(
            token_spot_prices_and_reserves_df[quantity_column],
            token_spot_prices_and_reserves_df[token_address_column],
        ):
            if (quantity_tuple is None) and (tokens_tuple is None):
                this_block_amounts = None
            else:
                this_block_amounts = []
                for q, t in zip(quantity_tuple, tokens_tuple):
                    # skip the pool token
                    if t.lower() != row["pool"].lower():
                        this_block_amounts.append(q)

            amounts_excluding_pool_token.append(this_block_amounts)

        sub_df = token_spot_prices_and_reserves_df[["block", token_spot_price_column]].copy()
        sub_df.columns = ["block", "spot_price"]
        sub_df["denominated_in"] = autopools[0].base_asset

        sub_df["quantity"] = amounts_excluding_pool_token

        sub_df["quantity"] = sub_df["quantity"].apply(
            lambda amounts: amounts[row["index"]] / (10 ** row["decimals"]) if amounts else None
        )
        sub_df["chain_id"] = chain.chain_id
        sub_df["token_address"] = row["token_address"]
        sub_df["destination_vault_address"] = row["destination_vault_address"]

        new_destination_token_values_rows.extend(
            [DestinationTokenValues.from_record(r) for r in sub_df.to_dict(orient="records")]
        )

    destination_info_df.apply(lambda row: _extract_destination_token_values(row), axis=1)

    idle_destination_token_values = _fetch_idle_destination_token_values(autopools, destination_info_df)

    insert_avoid_conflicts(
        [*new_destination_token_values_rows, *idle_destination_token_values],
        DestinationTokenValues,
        index_elements=[
            DestinationTokenValues.block,
            DestinationTokenValues.chain_id,
            DestinationTokenValues.token_address,
            DestinationTokenValues.destination_vault_address,
        ],
    )


def ensure_destination_token_values_are_current():

    for autopool in ALL_AUTOPOOLS:
        _fetch_and_insert_destination_token_values([autopool], autopool.chain)

    # for chain in ALL_CHAINS:
    #     for base_asset in ALL_BASE_ASSETS:
    #         autopools = [
    #             a for a in ALL_AUTOPOOLS_DATA_ON_CHAIN if a.chain == chain and a.base_asset == base_asset(chain)
    #         ]
    #         if autopools:
    #             _fetch_and_insert_destination_token_values(autopools, chain)

    # for chain in ALL_CHAINS:
    #     autopools = [a for a in ALL_AUTOPOOLS_DATA_ON_CHAIN if a.chain == chain]
    #     if autopools:
    #         _fetch_and_insert_destination_token_values(autopools, chain)

    #     autopools = [a for a in ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN if a.chain == chain]
    #     if autopools:
    #         _fetch_and_insert_destination_token_values(autopools, chain)


def _get_missing_idle_destination_token_values_needed_blocks(
    autopool: AutopoolConstants, destination_info_df: pd.DataFrame
) -> list[int]:
    idle_destination_token_values_df = get_full_table_as_df(
        DestinationTokenValues,
        where_clause=(DestinationTokenValues.destination_vault_address == autopool.autopool_eth_addr),
    )

    existing_blocks_by_destination = (
        idle_destination_token_values_df.groupby("destination_vault_address")["block"].apply(set).to_dict()
    )

    needed_blocks = set()

    for destination_vault_address, blocks_we_already_have in existing_blocks_by_destination.items():
        all_blocks_that_we_neeed = destination_info_df[
            destination_info_df["destination_vault_address"] == destination_vault_address
        ]["block"].unique()

        for block in all_blocks_that_we_neeed:
            if block not in blocks_we_already_have:
                needed_blocks.add(int(block))

    return list(needed_blocks)


def _fetch_idle_destination_token_values(
    autopools: list[AutopoolConstants], destination_info_df: pd.DataFrame
) -> list[DestinationTokenValues]:
    if len(autopools) != 1:
        raise ValueError("Autopools should only contain a single autopool")

    idle_calls = []
    for autopool in autopools:
        if autopool.base_asset in WETH:
            decimals = 18
        elif autopool.base_asset in USDC:
            decimals = 6

        def _asset_breakdown_to_idle(success, args):
            if success:
                totalIdle, totalDebt, totalDebtMin, totalDebtMax = args
                return int(totalIdle) / (10**decimals)

        idle_calls.append(
            Call(
                autopool.autopool_eth_addr,
                ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
                [(autopool.autopool_eth_addr, _asset_breakdown_to_idle)],
            )
        )

    missing_blocks = _get_missing_idle_destination_token_values_needed_blocks(autopools[0], destination_info_df)
    if not missing_blocks:
        return []
    idle_df = get_raw_state_by_blocks(idle_calls, missing_blocks, autopool.chain, include_block_number=True)

    idle_destination_token_values = []

    def _extract_idle_destination_token_values(row: dict):
        for autopool_vault_address, total_idle in row.items():
            if autopool_vault_address != "block":
                idle_destination_token_values.append(
                    DestinationTokenValues(
                        block=int(row["block"]),
                        chain_id=autopool.chain.chain_id,
                        destination_vault_address=autopool_vault_address,
                        token_address=autopool.base_asset,
                        spot_price=1.0,
                        quantity=total_idle,
                        denominated_in=autopool.base_asset,
                    )
                )

    idle_df.apply(_extract_idle_destination_token_values, axis=1)
    return idle_destination_token_values


if __name__ == "__main__":
    ensure_destination_token_values_are_current()
