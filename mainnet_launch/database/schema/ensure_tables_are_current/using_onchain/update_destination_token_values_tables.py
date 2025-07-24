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
    get_state_by_one_block,
)

from mainnet_launch.data_fetching.block_timestamp import ensure_all_blocks_are_in_table

from mainnet_launch.constants import (
    ALL_CHAINS,
    ROOT_PRICE_ORACLE,
    ChainData,
    AUTO_USD,
    ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN,
    ALL_AUTOPOOLS_DATA_ON_CHAIN,
    AutopoolConstants,
    WETH,
    USDC,
    time_decorator,
)


def _fetch_destination_token_value_data_from_external_source(
    chain: ChainData, destination_info_df: pd.DataFrame
) -> pd.DataFrame:

    #
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
    needed_blocks = [int(b) for b in destination_info_df["block"].unique()]

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


def _fetch_and_insert_destination_token_values(
    autopools: list[AutopoolConstants],
    chain: ChainData,
):

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

    # needs destination pool, destination lp otken address and destination_vault address
    token_spot_prices_and_reserves_df = _fetch_destination_token_value_data_from_external_source(
        chain, destination_info_df
    )

    new_destination_token_values_rows = []

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

    idle_destination_token_values = _fetch_idle_destination_token_values(
        autopools, destination_info_df["block"].unique()
    )

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
    for chain in ALL_CHAINS:
        autopools = [a for a in ALL_AUTOPOOLS_DATA_ON_CHAIN if a.chain == chain]
        if autopools:
            _fetch_and_insert_destination_token_values(autopools, chain)

        autopools = [a for a in ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN if a.chain == chain]
        if autopools:
            _fetch_and_insert_destination_token_values(autopools, chain)


def _fetch_idle_destination_token_values(
    autopools: list[AutopoolConstants], missing_blocks: list[int]
) -> list[DestinationTokenValues]:

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
            [
                Call(
                    autopool.autopool_eth_addr,
                    ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
                    [(autopool.autopool_eth_addr, _asset_breakdown_to_idle)],
                )
            ]
        )

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
                    )
                )

    idle_df.apply(_extract_idle_destination_token_values, axis=1)
    return idle_destination_token_values


if __name__ == "__main__":
    ensure_destination_token_values_are_current()
