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
    TableSelector,
    merge_tables_as_df,
)

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    identity_with_bool_success,
    safe_normalize_6_with_bool_success,
)

from mainnet_launch.constants import (
    ALL_CHAINS,
    ROOT_PRICE_ORACLE,
    ChainData,
    ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN,
    ALL_AUTOPOOLS_DATA_ON_CHAIN,
    AutopoolConstants,
    WETH,
    USDC,
    DOLA,
    AUTO_USD,
)


AUTO_USD_ROOT_PRICE_ORACLE = "0xdB8747a396D75D576Dc7a10bb6c8F02F4a3C20f1"


def _build_USD_autopool_price_calls(chain: ChainData, destination_info_df: pd.DataFrame) -> list[Call]:
    # pricer_contract.functions.getSpotPriceInQuote(underlyingTokens[i], pool, quote).call({}, blockNo)
    # note: this might need to be patched to include autopool.baseAsset -> 1.09
    pool_token_addresses = destination_info_df[
        destination_info_df["autopool_vault_address"] == AUTO_USD.autopool_eth_addr
    ][["pool", "token_address"]].drop_duplicates()
    return [
        Call(
            AUTO_USD_ROOT_PRICE_ORACLE,
            ["getSpotPriceInQuote(address,address,address)(uint256)", token_address, pool_address, USDC(chain)],
            [((pool_address, token_address, "spot_price"), safe_normalize_6_with_bool_success)],
        )
        for (pool_address, token_address) in zip(pool_token_addresses["pool"], pool_token_addresses["token_address"])
    ]


def _build_ETH_autopool_price_calls(chain: ChainData, destination_info_df: pd.DataFrame) -> list[Call]:
    pool_token_addresses = destination_info_df[
        destination_info_df["autopool_vault_address"] != AUTO_USD.autopool_eth_addr
    ][["pool", "token_address"]].drop_duplicates()

    return [
        Call(
            ROOT_PRICE_ORACLE(chain),
            ["getSpotPriceInEth(address,address)(uint256)", token_address, pool_address],
            [((pool_address, token_address, "spot_price"), safe_normalize_with_bool_success)],
        )
        for (pool_address, token_address) in zip(pool_token_addresses["pool"], pool_token_addresses["token_address"])
    ]


# this should be used but it is greyed out, not sure why
# I think this method is bad
# TODO bad method
def _build_DOLA_autopool_price_calls(chain: ChainData, destination_info_df: pd.DataFrame) -> list[Call]:
    # pricer_contract.functions.getSpotPriceInQuote(underlyingTokens[i], pool, quote).call({}, blockNo)
    # note: this might need to be patched to include autopool.baseAsset -> 1.09
    pool_token_addresses = destination_info_df[
        destination_info_df["autopool_vault_address"] == AUTO_USD.autopool_eth_addr
    ][["pool", "token_address"]].drop_duplicates()
    return [
        Call(
            AUTO_USD_ROOT_PRICE_ORACLE,
            ["getSpotPriceInQuote(address,address,address)(uint256)", token_address, pool_address, DOLA(chain)],
            [((pool_address, token_address, "spot_price"), safe_normalize_6_with_bool_success)],
        )
        for (pool_address, token_address) in zip(pool_token_addresses["pool"], pool_token_addresses["token_address"])
    ]


def _build_underlying_reserves_calls(destination_info_df: list[str]) -> list[Call]:
    unique_destinations = destination_info_df["destination_vault_address"].unique()
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


def _fetch_destination_token_value_data_from_external_source(
    chain: ChainData, destination_info_df: pd.DataFrame, missing_blocks: list[int]
) -> pd.DataFrame:

    usdc_destinations_spot_price_calls = _build_USD_autopool_price_calls(chain, destination_info_df)
    eth_destinations_spot_price_calls = _build_ETH_autopool_price_calls(chain, destination_info_df)
    underlying_reserves_calls = _build_underlying_reserves_calls(destination_info_df)

    df = get_raw_state_by_blocks(
        [*usdc_destinations_spot_price_calls, *eth_destinations_spot_price_calls, *underlying_reserves_calls],
        missing_blocks,
        chain,
        include_block_number=True,
    )

    return df


def _determine_what_blocks_are_needed(autopools: list[AutopoolConstants], chain: ChainData) -> list[int]:
    destination_state_df = merge_tables_as_df(
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
    )

    blocks_expected_to_have = destination_state_df["block"].unique()
    expected_destinations = destination_state_df["destination_vault_address"].unique()

    missing_blocks = get_subset_not_already_in_column(
        DestinationTokenValues,
        DestinationTokenValues.block,
        blocks_expected_to_have,
        where_clause=DestinationTokenValues.destination_vault_address.in_(expected_destinations),
    )

    return missing_blocks


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
                [Destinations.underlying, Destinations.pool, Destinations.denominated_in],
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
        ],
        where_clause=(AutopoolDestinations.chain_id == chain.chain_id)
        & (Destinations.pool_type != "idle")
        & (AutopoolDestinations.autopool_vault_address.in_([a.autopool_eth_addr for a in autopools])),
    ).drop_duplicates()

    missing_blocks = _determine_what_blocks_are_needed(autopools, chain)
    if not missing_blocks:
        return  # early stop

    # needs destination pool, destination lp otken address and destination_vault address
    token_spot_prices_and_reserves_df = _fetch_destination_token_value_data_from_external_source(
        chain, destination_info_df, missing_blocks
    )

    idle_destination_token_values = _fetch_idle_destination_token_values(autopools, missing_blocks)

    new_destination_token_values_rows = []

    unique_destination_info_df = destination_info_df[
        ["destination_vault_address", "token_address", "pool", "index", "decimals", "denominated_in"]
    ].drop_duplicates()

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

        sub_df["raw_quantity"] = amounts_excluding_pool_token

        sub_df["quantity"] = sub_df["raw_quantity"].apply(
            lambda amounts: amounts[row["index"]] / (10 ** row["decimals"]) if amounts else None
        )
        sub_df["chain_id"] = chain.chain_id
        sub_df["token_address"] = row["token_address"]
        sub_df["destination_vault_address"] = row["destination_vault_address"]
        sub_df["denominated_in"] = row["denominated_in"]

        new_destination_token_values_rows.extend(
            [DestinationTokenValues.from_record(r) for r in sub_df.to_dict(orient="records")]
        )

    unique_destination_info_df.apply(lambda row: _extract_destination_token_values(row), axis=1)

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

        def _asset_breakdown_to_idle(success, args):
            if success:
                totalIdle, totalDebt, totalDebtMin, totalDebtMax = args
                return int(totalIdle) / (10**autopool.base_asset_decimals)

        idle_calls.append(
            Call(
                autopool.autopool_eth_addr,
                ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
                [(autopool.autopool_eth_addr, _asset_breakdown_to_idle)],
            )
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
                        denominated_in=autopool.base_asset,
                    )
                )

    idle_df.apply(_extract_idle_destination_token_values, axis=1)
    return idle_destination_token_values


if __name__ == "__main__":
    ensure_destination_token_values_are_current()
