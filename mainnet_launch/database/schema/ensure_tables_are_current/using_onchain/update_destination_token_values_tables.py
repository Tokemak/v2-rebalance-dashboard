import pandas as pd
from multicall import Call


from mainnet_launch.database.schema.full import (
    DestinationTokenValues,
    DestinationTokens,
    Destinations,
    Tokens,
    DestinationStates,
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
    ROOT_PRICE_ORACLE,
    WETH,
    AUTO_USD,
    ALL_AUTOPOOLS,
    ChainData,
    AutopoolConstants,
    TokemakAddress,
)

# good enoough but is missing the values for the first few days towards the start
# # has getSpotPriceInQuote function

SOLVER_ROOT_ORACLE = TokemakAddress(
    eth="0xdB8747a396D75D576Dc7a10bb6c8F02F4a3C20f1",
    base="0x67D29b2d1b422922406d6d5fb7846aE99c282de1",
    sonic="0x4137b35266A4f42ad8B4ae21F14D0289861cc970",
)


def _build_get_spot_price_in_quote_calls(chain: ChainData, destination_info_df: pd.DataFrame) -> list[Call]:
    # pricer_contract.functions.getSpotPriceInQuote(underlyingTokens[i], pool, quote).call({}, blockNo)
    # note: this might need to be patched to include autopool.baseAsset -> 1.0
    pool_token_addresses = destination_info_df[
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


def _build_get_spot_price_in_eth_calls(chain: ChainData, destination_info_df: pd.DataFrame) -> list[Call]:
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

    eth_destinations_spot_price_calls = _build_get_spot_price_in_eth_calls(
        chain, destination_info_df[destination_info_df["base_asset"] == WETH(chain)]
    )
    non_eth_destinations_spot_price_calls = _build_get_spot_price_in_quote_calls(
        chain, destination_info_df[destination_info_df["base_asset"] != WETH(chain)]
    )
    underlying_reserves_calls = _build_underlying_reserves_calls(destination_info_df)

    df = get_raw_state_by_blocks(
        [*eth_destinations_spot_price_calls, *non_eth_destinations_spot_price_calls, *underlying_reserves_calls],
        missing_blocks,
        chain,
        include_block_number=True,
    )

    return df


def _determine_what_blocks_are_needed(autopool: AutopoolConstants) -> list[int]:
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
        where_clause=(DestinationStates.chain_id == autopool.chain.chain_id)
        & (AutopoolDestinations.autopool_vault_address == autopool.autopool_eth_addr),
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


def _fetch_and_insert_destination_token_values(autopool: AutopoolConstants):
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
        where_clause=(AutopoolDestinations.chain_id == autopool.chain.chain_id)
        & (Destinations.pool_type != "idle")
        & (AutopoolDestinations.autopool_vault_address == autopool.autopool_eth_addr),
    ).drop_duplicates()

    destination_info_df["base_asset"] = autopool.base_asset
    destination_info_df["base_asset_decimals"] = autopool.base_asset_decimals

    missing_blocks = _determine_what_blocks_are_needed(autopool)
    if not missing_blocks:
        return
    token_spot_prices_and_reserves_df = _fetch_destination_token_value_data_from_external_source(
        autopool.chain, destination_info_df, missing_blocks
    )

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
                    # skip the pool token for balancer composable stable pools
                    if t.lower() != row["pool"].lower():
                        this_block_amounts.append(q)

            amounts_excluding_pool_token.append(this_block_amounts)

        sub_df = token_spot_prices_and_reserves_df[["block", token_spot_price_column]].copy()
        sub_df.columns = ["block", "spot_price"]

        sub_df["raw_quantity"] = amounts_excluding_pool_token

        sub_df["quantity"] = sub_df["raw_quantity"].apply(
            lambda amounts: amounts[row["index"]] / (10 ** row["decimals"]) if amounts else None
        )
        sub_df["chain_id"] = autopool.chain.chain_id
        sub_df["token_address"] = row["token_address"]
        sub_df["destination_vault_address"] = row["destination_vault_address"]
        sub_df["denominated_in"] = row["denominated_in"]

        new_destination_token_values_rows.extend(
            [DestinationTokenValues.from_record(r) for r in sub_df.to_dict(orient="records")]
        )

    unique_destination_info_df.apply(lambda row: _extract_destination_token_values(row), axis=1)

    idle_destination_token_values = _fetch_idle_destination_token_values(autopool, missing_blocks)

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
        _fetch_and_insert_destination_token_values(autopool)


def _fetch_idle_destination_token_values(
    autopool: AutopoolConstants, missing_blocks: list[int]
) -> list[DestinationTokenValues]:
    def _asset_breakdown_to_idle(success, args):
        if success:
            totalIdle, totalDebt, totalDebtMin, totalDebtMax = args
            return int(totalIdle) / (10**autopool.base_asset_decimals)

    idle_calls = [
        Call(
            autopool.autopool_eth_addr,
            ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
            [(autopool.autopool_eth_addr, _asset_breakdown_to_idle)],
        )
    ]

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
