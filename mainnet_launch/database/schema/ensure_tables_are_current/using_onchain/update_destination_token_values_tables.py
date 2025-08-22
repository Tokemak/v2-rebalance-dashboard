import pandas as pd
from multicall import Call

from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import ARRAY, BIGINT, TEXT

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
    TableSelector,
    merge_tables_as_df,
    get_full_table_as_df,
    Session,
)

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    identity_with_bool_success,
    safe_normalize_6_with_bool_success,
)

from mainnet_launch.constants import (
    ROOT_PRICE_ORACLE,
    ALL_AUTOPOOLS,
    ChainData,
    AutopoolConstants,
    time_decorator,
    TokemakAddress,
)


SOLVER_ROOT_ORACLE = TokemakAddress(
    eth="0xdB8747a396D75D576Dc7a10bb6c8F02F4a3C20f1",
    base="0x67D29b2d1b422922406d6d5fb7846aE99c282de1",
    sonic="0x4137b35266A4f42ad8B4ae21F14D0289861cc970",
    name="SolverRootOracle",
)

# CONSIDER optimizing this process
# it takes 28 seconds when empty


# TODO consider moving these to another file
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


# def _build_underlying_reserves_calls(destination_address_info_df: pd.DataFrame) -> list[Call]:
#     unique_destinations = destination_address_info_df["destination_vault_address"].unique()
#     return [
#         Call(
#             destination_vault_address,
#             "underlyingReserves()(address[],uint256[])",
#             [
#                 ((destination_vault_address, "underlyingReserves_tokens"), identity_with_bool_success),
#                 ((destination_vault_address, "underlyingReserves_amounts"), identity_with_bool_success),
#             ],
#         )
#         for destination_vault_address in unique_destinations
#     ]


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

    destination_vault_addresses = destination_info_df["destination_vault_address"].unique().tolist()

    # we should have a destination token value for each destination state
    # this is not ideal, should be pure sql select distinct where
    desired_blocks = (
        get_full_table_as_df(
            DestinationStates,
            where_clause=DestinationStates.destination_vault_address.in_(destination_vault_addresses),
        )["block"]
        .unique()
        .tolist()
    )
    missing_blocks = get_needed_blocks_for_destination_token_values(
        destination_vault_addresses, desired_blocks, autopool.chain
    )
    if missing_blocks:

        token_spot_prices_and_reserves_df = _fetch_destination_token_value_data_from_external_source(
            autopool.chain, destination_info_df, missing_blocks
        )

        new_destination_token_values_rows = _convert_raw_token_spot_prices_and_reserves_df_to_new_rows(
            autopool, destination_info_df, token_spot_prices_and_reserves_df
        )

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

    idle_destination_token_values = _fetch_idle_destination_token_values(autopool)
    # only add rows if there are some to add
    if idle_destination_token_values:
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
    #  this can be done in one sql query, TODO fix later when optimizing for db reads
    destination_token_blocks_df = merge_tables_as_df(
        [
            TableSelector(
                DestinationTokenValues,
                select_fields=[DestinationTokenValues.block],
            )
        ],
        where_clause=(DestinationTokenValues.destination_vault_address == autopool.autopool_eth_addr),
    )

    destination_state_blocks_df = merge_tables_as_df(
        [
            TableSelector(
                DestinationStates,
                select_fields=[DestinationStates.block],
            )
        ],
        where_clause=(DestinationStates.destination_vault_address == autopool.autopool_eth_addr),
    )

    # Use sets for faster diff; coerce to int in case dtype is object
    desired_blocks = set(destination_state_blocks_df["block"].astype(int).tolist())
    existing_blocks = set(destination_token_blocks_df["block"].astype(int).tolist())

    needed_blocks = desired_blocks - existing_blocks

    return [int(b) for b in needed_blocks]


def _fetch_idle_destination_token_values(
    autopool: AutopoolConstants,
) -> list[DestinationTokenValues]:
    missing_blocks = _get_missing_idle_destination_token_values_needed_blocks(autopool)

    if not missing_blocks:
        return []

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


def ensure_destination_token_values_are_current():
    # in theory this can be done faster, in autopool groups but
    # I ran into issues with too many calls per http request
    # slower but more readable
    for autopool in ALL_AUTOPOOLS:
        _fetch_and_insert_destination_token_values(autopool)


if __name__ == "__main__":
    from mainnet_launch.constants import *

    profile_function(ensure_destination_token_values_are_current)
