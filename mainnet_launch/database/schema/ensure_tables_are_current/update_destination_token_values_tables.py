import pandas as pd
from multicall import Call


from mainnet_launch.database.schema.full import (
    DestinationTokenValues,
    DestinationTokens,
    Destinations,
    Tokens,
    TokenValues,
    Autopools,
)


from mainnet_launch.database.schema.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    natural_left_right_using_where,
    get_full_table_as_orm,
)

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
    identity_with_bool_success,
)

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
    destination_token_spot_price_df = get_raw_state_by_blocks(
        spot_price_calls, possible_blocks, chain, include_block_number=True
    )

    underlying_reserves_calls = build_underlying_reserves_calls(full_destination_df["destination_vault_address"])

    underlying_reserves_df = get_raw_state_by_blocks(
        underlying_reserves_calls, possible_blocks, chain, include_block_number=True
    )
    wide_df = pd.merge(destination_token_spot_price_df, underlying_reserves_df, on="block")
    return wide_df


def _build_all_destination_token_values(
    chain: ChainData, full_destination_df: pd.DataFrame, wide_df: pd.DataFrame, token_value_df: pd.DataFrame
) -> list[DestinationTokenValues]:
    all_destination_token_values = []

    def _build_destination_token_values(row: dict):
        if row["decimals"] == None:
            pass
        amounts = row["underlyingReserves_amounts"]
        index = row["index"]
        decimals = row["decimals"]

        if amounts is not None:
            quantity = amounts[index] / (10**decimals)
        else:
            quantity = None
        spot_price = row["spot_price"]
        safe_price = row["safe_price"]
        if (spot_price is not None) and (safe_price is not None):
            safe_spot_spread = (spot_price - safe_price) / safe_price
        else:
            safe_spot_spread = None

        if (spot_price is not None) and (row["backing"] is not None):
            spot_backing_discount = (spot_price - row["backing"]) / row["backing"]
        else:
            spot_backing_discount = None

        all_destination_token_values.append(
            DestinationTokenValues(
                block=row["block"],
                chain_id=chain.chain_id,
                destination_vault_address=row["destination_vault_address"],
                token_address=row["token_address"],
                spot_price=spot_price,
                quantity=quantity,
                safe_spot_spread=safe_spot_spread,
                spot_backing_discount=spot_backing_discount,
            )
        )

    for destination_vault_address, pool_address, token_address, index in zip(
        full_destination_df["destination_vault_address"],
        full_destination_df["pool"],
        full_destination_df["token_address"],
        full_destination_df["index"],
    ):
        cols = [
            (pool_address, token_address, "spot_price"),
            (destination_vault_address, "underlyingReserves_amounts"),
            "block",
        ]

        this_destination_token_df = wide_df[cols].copy()
        this_destination_token_df.columns = ["spot_price", "underlyingReserves_amounts", "block"]
        this_destination_token_df["token_address"] = token_address
        this_destination_token_df["index"] = index
        this_destination_token_df["destination_vault_address"] = destination_vault_address
        this_destination_token_df = pd.merge(
            this_destination_token_df, token_value_df, on=["block", "token_address"], how="left"
        )
        this_destination_token_df.apply(_build_destination_token_values, axis=1)

    return all_destination_token_values


def ensure_destination_token_values_are_current():
    for chain in ALL_CHAINS:
        possible_blocks = build_blocks_to_use(chain)

        missing_blocks = get_subset_not_already_in_column(
            DestinationTokenValues,
            DestinationTokenValues.block,
            possible_blocks,
            where_clause=DestinationTokenValues.chain_id == chain.chain_id,
        )
        # missing_blocks = possible_blocks[::21]
        if len(missing_blocks) == 0:
            continue

        full_destination_df = natural_left_right_using_where(
            DestinationTokens,
            Destinations,
            using=[DestinationTokens.destination_vault_address, DestinationTokens.chain_id],
            where_clause=(DestinationTokens.chain_id == chain.chain_id) & (Destinations.pool_type != "idle"),
        )
        token_value_df = natural_left_right_using_where(
            TokenValues,
            Tokens,
            using=[TokenValues.token_address, TokenValues.chain_id],
            where_clause=TokenValues.chain_id == chain.chain_id,
        )

        wide_df = _fetch_destination_token_value_data_from_external_source(chain, missing_blocks, full_destination_df)

        all_destination_token_values = _build_all_destination_token_values(
            chain, full_destination_df, wide_df, token_value_df
        )
        insert_avoid_conflicts(
            all_destination_token_values,
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
                        safe_spot_spread=0.0,
                        spot_backing_discount=0.0,
                    )
                )

    idle_df.apply(_extract_idle_destination_token_values, axis=1)
    return idle_destination_token_values


if __name__ == "__main__":
    ensure_destination_token_values_are_current()
