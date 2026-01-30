import pandas as pd
from multicall import Call
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import ARRAY, BIGINT, TEXT

from mainnet_launch.database.schema.full import (
    Autopools,
    DestinationStates,
    Destinations,
    AutopoolDestinations,
    RebalanceEvents,
)


from mainnet_launch.database.postgres_operations import (
    get_full_table_as_orm,
    insert_avoid_conflicts,
    merge_tables_as_df,
    set_some_cells_to_null,
    TableSelector,
)
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
)

from mainnet_launch.database.schema.full import Session

from mainnet_launch.constants import (
    ChainData,
    ALL_CHAINS,
    POINTS_HOOK,
    ROOT_PRICE_ORACLE,
    ALL_AUTOPOOLS_DATA_ON_CHAIN,
    ALL_AUTOPOOLS,
    AutopoolConstants,
)


def build_lp_token_spot_and_safe_price_calls(
    destination_addresses: list[str],
    lp_token_addresses: list[str],
    pool_addresses: list[str],
    autopool: AutopoolConstants,
) -> list[Call]:

    def _handle_getRangePricesLP(success, args):
        if success:
            spotPriceInQuote, safePriceInQuote, isSpotSafe = args
            lp_token_spot_price = spotPriceInQuote / (10**autopool.base_asset_decimals)
            lp_token_safe_price = safePriceInQuote / (10**autopool.base_asset_decimals)
            return (lp_token_spot_price, lp_token_safe_price)

    return [
        Call(
            ROOT_PRICE_ORACLE(autopool.chain),
            [
                "getRangePricesLP(address,address,address)((uint256,uint256,uint256))",
                lp_token,
                pool,
                autopool.base_asset,
            ],
            [((destination, "lp_token_spot_and_safe"), _handle_getRangePricesLP)],
        )
        for destination, lp_token, pool in zip(destination_addresses, lp_token_addresses, pool_addresses)
    ]


# TODO add the price values here to the pie charts on the % ownership page


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

    autopool_vault_address_to_autopool = {a.autopool_eth_addr: a for a in ALL_AUTOPOOLS}

    for autopool_vault_address in autopool_to_all_ever_active_destinations.keys():
        this_autopool_active_destinations: list[Destinations] = [
            dest
            for dest in destination_orm
            if dest.destination_vault_address in autopool_to_all_ever_active_destinations[autopool_vault_address]
        ]

        destination_vault_addresses = [dest.destination_vault_address for dest in this_autopool_active_destinations]
        lp_token_addresses = [dest.underlying for dest in this_autopool_active_destinations]
        pool_addresses = [dest.pool for dest in this_autopool_active_destinations]

        calls = build_lp_token_spot_and_safe_price_calls(
            destination_vault_addresses,
            lp_token_addresses,
            pool_addresses,
            autopool_vault_address_to_autopool[autopool_vault_address],
        )
        lp_token_spot_prices_calls.extend(calls)

    lp_token_spot_price_df = get_raw_state_by_blocks(
        lp_token_spot_prices_calls, missing_blocks, chain, include_block_number=True
    )
    return lp_token_spot_price_df


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
    # currently broken
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
                    rebalance_plan_timestamp=None,
                    rebalance_plan_key=None,
                )
                all_new_destination_states.append(new_destination_state)

            raw_destination_states_df.apply(_extract_destination_states, axis=1)

    return all_new_destination_states


def get_needed_blocks_pure_sql(desired_blocks: list[int], chain: ChainData) -> list[int]:
    sql = text(
        """
    WITH desired_blocks AS (
        SELECT UNNEST(CAST(:desired_blocks AS bigint[])) AS block
        ),
    destinations AS (
      SELECT DISTINCT d.destination_vault_address
      FROM autopool_destinations ad
      JOIN destinations d
        ON ad.destination_vault_address = d.destination_vault_address
      WHERE d.chain_id = :chain_id
        AND ad.autopool_vault_address = ANY(CAST(:autopool_addrs AS text[]))
    ),
    present AS (
      SELECT db.block, ds.destination_vault_address
      FROM desired_blocks db
      JOIN destination_states ds
        ON ds.block = db.block
       AND ds.chain_id = :chain_id
       AND ds.destination_vault_address IN (SELECT destination_vault_address FROM destinations)
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

    # Keep casts in SQL, but also type the binds (safer with empty lists, etc.)
    sql = sql.bindparams(
        bindparam("desired_blocks", type_=ARRAY(BIGINT)),
        bindparam("autopool_addrs", type_=ARRAY(TEXT)),
        bindparam("chain_id"),
    )

    autopool_addrs = [a.autopool_eth_addr for a in ALL_AUTOPOOLS_DATA_ON_CHAIN]

    with Session.begin() as session:
        return (
            session.execute(
                sql,
                {
                    "desired_blocks": desired_blocks,
                    "chain_id": chain.chain_id,
                    "autopool_addrs": autopool_addrs,
                },
            )
            .scalars()
            .all()
        )


def _add_new_destination_states_to_db(desired_blocks: list[int], chain: ChainData):
    missing_blocks = get_needed_blocks_pure_sql(desired_blocks, chain)
    if not missing_blocks:
        return

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

    if autopool_and_destinations_df.empty:
        # if there are no autopools on this chain that we are using the onchain sources
        # instead of the rebalance plan sources then early exit
        return

    autopool_to_all_ever_active_destinations = (
        autopool_and_destinations_df.groupby("autopool_vault_address")["destination_vault_address"]
        .apply(tuple)
        .to_dict()
    )

    destination_underlying_total_supply_df = _fetch_destination_total_supply_df(
        autopool_to_all_ever_active_destinations, missing_blocks, chain
    )
    # points are depreacted, can remove
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

    autopool_vault_addresses = [k for k in autopool_to_all_ever_active_destinations.keys()]

    idle_destination_states = _fetch_idle_destination_states(chain, autopool_vault_addresses, missing_blocks)

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
    chain: ChainData, autopool_vault_addresses: list[str], missing_blocks: list[int]
) -> list[DestinationStates]:
    idle_destination_states = []
    for autopool_vault_address in autopool_vault_addresses:
        for block in missing_blocks:
            idle_destination_states.append(
                DestinationStates(
                    destination_vault_address=autopool_vault_address,
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
                    rebalance_plan_timestamp=None,
                    rebalance_plan_key=None,
                )
            )
    return idle_destination_states


def _overwrite_bad_summary_states_rows():
    other_values_set_to_null = {
        "incentive_apr": None,
        "fee_apr": None,
        "base_apr": None,
        "points_apr": None,
        "fee_plus_base_apr": None,
        "total_apr_in": None,
        "total_apr_out": None,
        "underlying_token_total_supply": None,
        "safe_total_supply": None,
        "lp_token_spot_price": None,
        "lp_token_safe_price": None,
        "from_rebalance_plan": None,
        "rebalance_plan_timestamp": None,
        "rebalance_plan_key": None,
    }
    # todo, consider a non manual way to do this
    bad_rows = [
        DestinationStates(
            destination_vault_address="0x49895f72fd9d0BF6BBb485C70CE38556de62b070",
            block=22385293,
            chain_id=1,
            **other_values_set_to_null,
        ),
        DestinationStates(
            destination_vault_address="0x3F55eedDe51504E6Ed0ec30E8289b4Da11EdB7F9",
            block=22385293,
            chain_id=1,
            **other_values_set_to_null,
        ),
        DestinationStates(
            destination_vault_address="0x3F55eedDe51504E6Ed0ec30E8289b4Da11EdB7F9",
            block=22442219,
            chain_id=1,
            **other_values_set_to_null,
        ),
        DestinationStates(
            destination_vault_address="0x49895f72fd9d0BF6BBb485C70CE38556de62b070",
            block=22442219,
            chain_id=1,
            **other_values_set_to_null,
        ),
        DestinationStates(
            destination_vault_address="0x5c6aeb9ef0d5BbA4E6691f381003503FD0D45126",
            block=21339732,
            chain_id=1,
            **other_values_set_to_null,
        ),
        DestinationStates(
            destination_vault_address="0x60339056EC88996e41757E05a798310E46972cca",
            block=21311105,
            chain_id=1,
            **other_values_set_to_null,
        ),
        DestinationStates(
            destination_vault_address="0xB6d68122428Dc1141467cB96791618615Ab9F746",
            block=21311105,
            chain_id=1,
            **other_values_set_to_null,
        ),
        DestinationStates(
            destination_vault_address="0xB6d68122428Dc1141467cB96791618615Ab9F746",
            block=21296777,
            chain_id=1,
            **other_values_set_to_null,
        ),
        DestinationStates(
            destination_vault_address="0x60339056EC88996e41757E05a798310E46972cca",
            block=21296777,
            chain_id=1,
            **other_values_set_to_null,
        ),
        DestinationStates(
            destination_vault_address="0x60339056EC88996e41757E05a798310E46972cca",
            block=21303933,
            chain_id=1,
            **other_values_set_to_null,
        ),
        DestinationStates(
            destination_vault_address="0xB6d68122428Dc1141467cB96791618615Ab9F746",
            block=21303933,
            chain_id=1,
            **other_values_set_to_null,
        ),
        DestinationStates(
            destination_vault_address="0x3F55eedDe51504E6Ed0ec30E8289b4Da11EdB7F9",
            block=22506180,
            chain_id=1,
            **other_values_set_to_null,
        ),
        DestinationStates(
            destination_vault_address="0x49895f72fd9d0BF6BBb485C70CE38556de62b070",
            block=22506180,
            chain_id=1,
            **other_values_set_to_null,
        ),
        DestinationStates(
            destination_vault_address="0x49895f72fd9d0BF6BBb485C70CE38556de62b070",
            block=22499048,
            chain_id=1,
            **other_values_set_to_null,
        ),
        DestinationStates(
            destination_vault_address="0x3F55eedDe51504E6Ed0ec30E8289b4Da11EdB7F9",
            block=22499048,
            chain_id=1,
            **other_values_set_to_null,
        ),
    ]
    # helper sql to see what destination states are wrong

    #     SELECT *
    # FROM destination_states
    # where incentive_apr > 0
    # ORDER BY incentive_apr DESC
    # LIMIT 5;
    set_some_cells_to_null(
        table=DestinationStates,
        rows=bad_rows,
        cols_to_null=[
            DestinationStates.incentive_apr,
            DestinationStates.total_apr_in,
            DestinationStates.total_apr_out,
        ],
    )


def get_rebalance_blocks(chain) -> list[int]:
    query = f"""
        SELECT DISTINCT t.block
        FROM rebalance_events re
        JOIN transactions t
          ON t.tx_hash = re.tx_hash
         AND t.chain_id = re.chain_id
        WHERE re.chain_id = {chain.chain_id}
        ORDER BY t.block
    """

    blocks = list(_exec_sql_and_cache(query)["block"])
    return blocks


def ensure_destination_states_are_current():
    for chain in ALL_CHAINS:
        possible_blocks = build_blocks_to_use(chain)
        # blocks_with_rebalances = get_rebalance_blocks(chain)
        # possible_blocks = list(set(possible_blocks).union(set(blocks_with_rebalances)))
        _add_new_destination_states_to_db(possible_blocks, chain)

    _overwrite_bad_summary_states_rows()


if __name__ == "__main__":

    from mainnet_launch.constants import profile_function

    # profile_function(ensure_destination_states_are_current)

    ensure_destination_states_are_current()
