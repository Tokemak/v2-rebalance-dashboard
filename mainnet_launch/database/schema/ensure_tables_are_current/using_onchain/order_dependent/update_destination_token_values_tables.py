import pandas as pd
from multicall import Call
from concurrent.futures import ThreadPoolExecutor


from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import ARRAY, BIGINT, TEXT  # TODO don't use these

from mainnet_launch.database.schema.full import (
    DestinationTokenValues,
    DestinationTokens,
    Destinations,
    Tokens,
    AutopoolDestinations,
)


from mainnet_launch.database.postgres_operations import (
    insert_avoid_conflicts,
    TableSelector,
    merge_tables_as_df,
    _exec_sql_and_cache,
    Session,
)

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    identity_with_bool_success,
)

from mainnet_launch.constants import (
    ROOT_PRICE_ORACLE,
    ALL_AUTOPOOLS,
    ChainData,
    AutopoolConstants,
    time_decorator,
    TokemakAddress,
    SOLVER_ROOT_ORACLE,
)


# TODO think about how to optimize this,
# rewrite the sql for clarity
# TODO understand and rewrite each sql query


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

    # seperate because too many calls in each group
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


def get_needed_blocks_for_destination_token_values(
    destination_vault_addresses: list[str], desired_blocks: list[int], chain: ChainData
) -> list[int]:
    sql = text(
        """
    WITH desired_blocks AS (
    SELECT UNNEST(CAST(:desired_blocks AS bigint[])) AS block
    ),
    destinations AS (
    SELECT UNNEST(CAST(:destination_vault_addresses AS text[])) AS destination_vault_address
    ),
    present AS (
    SELECT
        db.block,
        dtv.destination_vault_address
    FROM desired_blocks db
    JOIN destination_token_values dtv
        ON dtv.block = db.block
    AND dtv.chain_id = :chain_id
    AND dtv.destination_vault_address IN (SELECT destination_vault_address FROM destinations)
    )
    SELECT db.block
    FROM desired_blocks db
    CROSS JOIN (SELECT COUNT(*) AS total_dests FROM destinations) t
    LEFT JOIN (
    SELECT block, COUNT(DISTINCT destination_vault_address) AS have
    FROM present
    GROUP BY block
    ) h ON h.block = db.block
    WHERE COALESCE(h.have, 0) < t.total_dests
    ORDER BY db.block;
    """
    )

    # Keep casts in SQL, but also type the binds (handles empty lists safely).
    sql = sql.bindparams(
        bindparam("desired_blocks", type_=ARRAY(BIGINT)),
        bindparam("destination_vault_addresses", type_=ARRAY(TEXT)),
        bindparam("chain_id"),
    )

    with Session.begin() as session:
        missing_blocks = (
            session.execute(
                sql,
                {
                    "desired_blocks": desired_blocks,  # list[int]
                    "destination_vault_addresses": destination_vault_addresses,  # list[str]
                    "chain_id": chain.chain_id,  # int
                },
            )
            .scalars()
            .all()
        )
        return missing_blocks


def _get_desired_blocks(destination_vault_addresses: list[str]) -> list[int]:
    if not destination_vault_addresses:
        return []

    # Safely format list of addresses for SQL IN clause
    in_clause = ", ".join(f"'{addr}'" for addr in destination_vault_addresses)

    sql = f"""
        SELECT DISTINCT block
        FROM destination_states
        WHERE destination_vault_address IN ({in_clause})
        ORDER BY block;
    """

    df = _exec_sql_and_cache(sql)
    return df["block"].astype(int).tolist() if not df.empty else []


def _get_destination_info_df(autopool: AutopoolConstants) -> pd.DataFrame:
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
        ],
        where_clause=(Destinations.chain_id == autopool.chain.chain_id)
        & (Destinations.pool_type != "idle")
        & (AutopoolDestinations.autopool_vault_address == autopool.autopool_eth_addr),
    )
    return destination_info_df


def _fetch_and_insert_non_idle_destination_token_values(autopool: AutopoolConstants):
    destination_info_df = _get_destination_info_df(autopool)
    destination_vault_addresses = destination_info_df["destination_vault_address"].unique().tolist()
    desired_blocks = _get_desired_blocks(destination_vault_addresses)
    missing_blocks = get_needed_blocks_for_destination_token_values(
        destination_vault_addresses, desired_blocks, autopool.chain
    )
    if not missing_blocks:
        # early exit on not missing any blocks
        return

    token_spot_prices_and_reserves_df = _fetch_destination_token_value_data_from_external_source(
        autopool.chain, destination_info_df, missing_blocks
    )

    new_destination_token_values_rows = _convert_raw_token_spot_prices_and_reserves_df_to_new_rows(
        autopool, destination_info_df, token_spot_prices_and_reserves_df
    )

    insert_avoid_conflicts(new_destination_token_values_rows, DestinationTokenValues)
    print(f'wrote {len(new_destination_token_values_rows):,} non-idle destination token values for {autopool.name}')


def _convert_raw_token_spot_prices_and_reserves_df_to_new_rows(
    autopool: AutopoolConstants, destination_info_df: pd.DataFrame, token_spot_prices_and_reserves_df: pd.DataFrame
) -> list[DestinationTokenValues]:

    new_destination_token_values_rows = []

    # this is really show, unsure why 90 seconds # double checked and this is not actaually slow
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
        sub_df["denominated_in"] = autopool.base_asset

        sub_df["quantity"] = amounts_excluding_pool_token

        sub_df["quantity"] = sub_df["quantity"].apply(
            lambda amounts: amounts[row["index"]] / (10 ** row["decimals"]) if amounts else None
        )
        sub_df["chain_id"] = autopool.chain.chain_id
        sub_df["token_address"] = row["token_address"]
        sub_df["destination_vault_address"] = row["destination_vault_address"]

        new_destination_token_values_rows.extend(
            [DestinationTokenValues.from_record(r) for r in sub_df.to_dict(orient="records")]
        )

    destination_info_df.apply(lambda row: _extract_destination_token_values(row), axis=1)
    return new_destination_token_values_rows


def _get_missing_idle_destination_token_values_needed_blocks(
    autopool: AutopoolConstants,
) -> list[int]:
    sql = f"""
        SELECT ds.block
        FROM destination_states AS ds
        LEFT JOIN destination_token_values AS dtv
          ON dtv.block = ds.block
         AND dtv.chain_id = ds.chain_id
         AND dtv.destination_vault_address = ds.destination_vault_address
        WHERE ds.destination_vault_address = '{autopool.autopool_eth_addr}'
          AND ds.chain_id = {autopool.chain.chain_id}
          AND dtv.block IS NULL
        ORDER BY ds.block;
    """

    df = _exec_sql_and_cache(sql)
    if df.empty:
        return []
    return df["block"].astype(int).tolist()


def _fetch_and_insert_idle_destination_token_values(
    autopool: AutopoolConstants,
) -> list[DestinationTokenValues]:
    missing_blocks = _get_missing_idle_destination_token_values_needed_blocks(autopool)

    if not missing_blocks:
        return

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

    insert_avoid_conflicts(
        idle_destination_token_values,
        DestinationTokenValues,
    )
    print(f'wrote {len(idle_destination_token_values):,} idle destination token values for {autopool.name}')


def ensure_destination_token_values_are_current():
    for autopool in ALL_AUTOPOOLS:
        _fetch_and_insert_non_idle_destination_token_values(autopool)
        _fetch_and_insert_idle_destination_token_values(autopool)


if __name__ == "__main__":
    from mainnet_launch.constants import *

    # profile_function(_fetch_and_insert_destination_token_values, AUTO_USD) # 5 seconds

    # profile_function(_fetch_and_insert_non_idle_destination_token_values, BASE_USD)
    # profile_function(_fetch_and_insert_idle_destination_token_values, BASE_USD)

    profile_function(ensure_destination_token_values_are_current)
    # ensure_destination_token_values_are_current()


# def _get_missing_idle_destination_token_values_needed_blocks(
#     autopool: AutopoolConstants,
# ) -> list[int]:
#     #  this can be done in one sql query, TODO fix later when optimizing for db reads
#     destination_token_blocks_df = merge_tables_as_df(
#         [
#             TableSelector(
#                 DestinationTokenValues,
#                 select_fields=[DestinationTokenValues.block],
#             )
#         ],
#         where_clause=(DestinationTokenValues.destination_vault_address == autopool.autopool_eth_addr),
#     )

#     destination_state_blocks_df = merge_tables_as_df(
#         [
#             TableSelector(
#                 DestinationStates,
#                 select_fields=[DestinationStates.block],
#             )
#         ],
#         where_clause=(DestinationStates.destination_vault_address == autopool.autopool_eth_addr),
#     )

#     # Use sets for faster diff; coerce to int in case dtype is object
#     desired_blocks = set(destination_state_blocks_df["block"].astype(int).tolist())
#     existing_blocks = set(destination_token_blocks_df["block"].astype(int).tolist())

#     needed_blocks = desired_blocks - existing_blocks

#     return [int(b) for b in needed_blocks]
