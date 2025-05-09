import pandas as pd
from multicall import Call


from mainnet_launch.database.schema.full import (
    DestinationTokenValues,
    DestinationTokens,
    Destinations,
    Tokens,
    DestinationStates,
    Autopools,
)


from mainnet_launch.database.schema.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    natural_left_right_using_where,
    get_full_table_as_orm,
    TableSelector,
    merge_tables_as_df,
)

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
    identity_with_bool_success,
)

from mainnet_launch.data_fetching.block_timestamp import ensure_all_blocks_are_in_table

from mainnet_launch.constants import ALL_CHAINS, ROOT_PRICE_ORACLE, ChainData


def _fetch_destination_token_value_data_from_external_source(
    chain: ChainData, possible_blocks: list[int], full_destination_df: pd.DataFrame
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

    spot_price_calls = build_pool_token_spot_price_calls(
        chain, full_destination_df["pool"], full_destination_df["token_address"]
    )

    underlying_reserves_calls = build_underlying_reserves_calls(full_destination_df["destination_vault_address"])

    return get_raw_state_by_blocks(
        [*spot_price_calls, *underlying_reserves_calls], possible_blocks, chain, include_block_number=True
    )


def _fetch_and_insert_destination_token_values(chain: ChainData, possible_blocks: list[int]):
    missing_blocks = get_subset_not_already_in_column(
        DestinationTokenValues,
        DestinationTokenValues.block,
        possible_blocks,
        where_clause=DestinationTokenValues.chain_id == chain.chain_id,
    )
    if len(missing_blocks) == 0:
        return

    ensure_all_blocks_are_in_table(missing_blocks, chain)

    destinations_df = merge_tables_as_df(
        [
            TableSelector(DestinationTokens, [DestinationTokens.token_address, DestinationTokens.index]),
            TableSelector(
                Destinations,
                [DestinationTokens.destination_vault_address, Destinations.pool],
                join_on=(Destinations.chain_id == DestinationTokens.chain_id)
                & (Destinations.destination_vault_address == DestinationTokens.destination_vault_address),
            ),
            TableSelector(
                Tokens,
                [Tokens.decimals],
                join_on=(Tokens.chain_id == DestinationTokens.chain_id)
                & (Tokens.token_address == DestinationTokens.token_address),
            ),
        ],
        where_clause=(DestinationTokens.chain_id == chain.chain_id) & (Destinations.pool_type != "idle"),
    )

    token_spot_prices_and_reserves_df = _fetch_destination_token_value_data_from_external_source(
        chain, missing_blocks, destinations_df
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

        if (sub_df["quantity"] > 1_000_000).any():
            pass

        new_destination_token_values_rows.extend(
            [DestinationTokenValues.from_record(r) for r in sub_df.to_dict(orient="records")]
        )

    destinations_df.apply(lambda row: _extract_destination_token_values(row), axis=1)

    insert_avoid_conflicts(
        new_destination_token_values_rows,
        DestinationTokenValues,
        index_elements=[
            DestinationTokenValues.block,
            DestinationTokenValues.chain_id,
            DestinationTokenValues.token_address,
            DestinationTokenValues.destination_vault_address,
        ],
    )

    # primary source of idle
    idle_destination_token_values = _fetch_idle_destination_token_values(chain, missing_blocks)

    insert_avoid_conflicts(
        idle_destination_token_values,
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
        # get all all the blocks we've already fetched
        # and then add any blocks htat are in destiantion states but not in destination token values
        already_fetched_blocks = get_subset_not_already_in_column(
            DestinationTokenValues,
            DestinationTokenValues.block,
            [],
            where_clause=DestinationTokenValues.chain_id == chain.chain_id,
        )

        possible_blocks = get_subset_not_already_in_column(
            DestinationStates,
            DestinationStates.block,
            already_fetched_blocks,
            where_clause=DestinationStates.chain_id == chain.chain_id,
        )

        _fetch_and_insert_destination_token_values(chain, possible_blocks)


def _fetch_idle_destination_token_values(chain: ChainData, missing_blocks: list[int]) -> list[DestinationTokenValues]:

    autopools: list[Autopools] = get_full_table_as_orm(
        Autopools,
        where_clause=Autopools.chain_id == chain.chain_id,
    )

    def _asset_breakdown_to_idle(success, args):
        if success:
            totalIdle, totalDebt, totalDebtMin, totalDebtMax = args
            return int(totalIdle) / 1e18  # maybe 1e6 if autoUSD

    idle_calls = [
        Call(
            autopool.autopool_vault_address,
            ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
            [(autopool.autopool_vault_address, _asset_breakdown_to_idle)],
        )
        for autopool in autopools
    ]

    idle_df = get_raw_state_by_blocks(idle_calls, missing_blocks, chain, include_block_number=True)

    idle_destination_token_values = []

    def _extract_idle_destination_token_values(row: dict):
        for autopool_vault_address, total_idle in row.items():
            if autopool_vault_address != "block":
                this_autopool = [a for a in autopools if a.autopool_vault_address == autopool_vault_address][0]
                idle_destination_token_values.append(
                    DestinationTokenValues(
                        block=int(row["block"]),
                        chain_id=chain.chain_id,
                        destination_vault_address=autopool_vault_address,
                        token_address=this_autopool.base_asset,
                        spot_price=1.0,
                        quantity=total_idle,
                    )
                )

    idle_df.apply(_extract_idle_destination_token_values, axis=1)
    return idle_destination_token_values


if __name__ == "__main__":
    ensure_destination_token_values_are_current()
