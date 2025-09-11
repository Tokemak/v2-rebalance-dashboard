from __future__ import annotations


from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


from multicall import Call
import pandas as pd


from mainnet_launch.constants import (
    AutopoolConstants,
    ChainData,
    ALL_AUTOPOOLS,
    WETH,
    ROOT_PRICE_ORACLE,
    SOLVER_ROOT_ORACLE,
)
from mainnet_launch.database.schema.full import (
    RebalancePlans,
    Destinations,
    AutopoolStates,
    Tokens,
    RebalanceEvents,
    Transactions,
)
from mainnet_launch.database.postgres_operations import (
    get_full_table_as_df,
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    TableSelector,
    merge_tables_as_df,
)
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    get_state_by_one_block,
    safe_normalize_with_bool_success,
    safe_normalize_6_with_bool_success,
)
from mainnet_launch.data_fetching.tokemak_subgraph import fetch_autopool_rebalance_events_from_subgraph
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_destinations_states_table import (
    build_lp_token_spot_and_safe_price_calls,
)
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_autopool_states import (
    _fetch_new_autopool_state_rows,
)

from mainnet_launch.database.schema.full import AutopoolDestinations, Destinations, DestinationTokens


# could you instead use? looking at the diff in destination vault total supply?
# assumes that there cannot be two rebalances in the same block


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


def _connect_plans_to_rebalance_events(
    rebalance_event_df: pd.DataFrame,
    rebalance_plan_df: pd.DataFrame,
) -> dict:
    rebalance_transaction_hash_to_rebalance_plan = {}

    for index in range(len(rebalance_event_df)):
        one_rebalance_event = rebalance_event_df.iloc[index]

        tx_hash = one_rebalance_event["transactionHash"]
        same_destinations = (rebalance_plan_df["token_out"] == one_rebalance_event["tokenOutAddress"]) & (
            rebalance_plan_df["token_in"] == one_rebalance_event["tokenInAddress"]
        )
        same_amount_out = rebalance_plan_df["amount_out"] == one_rebalance_event["tokenOutAmount"]

        window_start = one_rebalance_event["datetime_executed"] - pd.Timedelta(minutes=10)
        generated_no_more_than_one_hour_before = rebalance_plan_df["datetime_generated"].between(
            window_start, one_rebalance_event["datetime_executed"]
        )

        matches = rebalance_plan_df[same_destinations & same_amount_out & generated_no_more_than_one_hour_before]
        matches = matches.sort_values("datetime_generated", ascending=False).head(1)

        if matches.empty:
            rebalance_transaction_hash_to_rebalance_plan[tx_hash] = None
        else:
            rebalance_transaction_hash_to_rebalance_plan[tx_hash] = matches["file_name"].values[0]

    return rebalance_transaction_hash_to_rebalance_plan


def _load_destination_info_df(autopool: AutopoolConstants) -> pd.DataFrame:
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
                table=DestinationTokens,
                select_fields=[
                    DestinationTokens.token_address,
                ],
                join_on=DestinationTokens.destination_vault_address == AutopoolDestinations.destination_vault_address,
            ),
            TableSelector(
                table=Destinations,
                select_fields=[Destinations.pool],
                join_on=Destinations.destination_vault_address == AutopoolDestinations.destination_vault_address,
            ),
            TableSelector(
                table=Tokens,
                select_fields=[Tokens.decimals, Tokens.name],
                join_on=Tokens.token_address == DestinationTokens.token_address,
            ),
        ],
        where_clause=AutopoolDestinations.autopool_vault_address == autopool.autopool_eth_addr,
    )
    return destination_info_df


def _load_raw_rebalance_event_df(autopool: AutopoolConstants):
    """Gets the data from the subgraph"""

    # TODO convert to sql, only fetch from subgraph based on where.
    # query: get all rebalance events where the tx hash is not in (list)

    # these are dominating time costs
    rebalance_event_df = fetch_autopool_rebalance_events_from_subgraph(autopool)

    # these are dominating time costs
    rebalance_plan_df = get_full_table_as_df(
        RebalancePlans,
        where_clause=(RebalancePlans.autopool_vault_address == autopool.autopool_eth_addr),
    )

    all_rebalance_event_hashes = rebalance_event_df["transactionHash"].to_list()

    rebalance_event_hashes_to_fetch = get_subset_not_already_in_column(
        RebalanceEvents,
        RebalanceEvents.tx_hash,
        all_rebalance_event_hashes,
        where_clause=RebalanceEvents.autopool_vault_address == autopool.autopool_eth_addr,
    )

    rebalance_event_df = rebalance_event_df[
        rebalance_event_df["transactionHash"].isin(rebalance_event_hashes_to_fetch)
    ].copy()

    hash_to_plan = _connect_plans_to_rebalance_events(rebalance_event_df, rebalance_plan_df)

    rebalance_event_df["rebalance_file_path"] = rebalance_event_df["transactionHash"].map(hash_to_plan)
    rebalance_event_df["autopool_vault_address"] = autopool.autopool_eth_addr
    rebalance_event_df["chain_id"] = autopool.chain.chain_id
    return rebalance_event_df


def ensure_rebalance_events_are_current():
    for autopool in ALL_AUTOPOOLS:
        # dominating time cost here 50 seconds
        rebalance_event_df = _load_raw_rebalance_event_df(autopool)  # hits the subgraph

        if rebalance_event_df.empty:
            print(autopool.name, "no new rebalance events to fetch")
            continue

        ensure_all_transactions_are_saved_in_db(rebalance_event_df["transactionHash"].to_list(), autopool.chain)

        transaction_df = get_full_table_as_df(
            Transactions, where_clause=Transactions.tx_hash.in_(rebalance_event_df["transactionHash"].to_list())
        )

        tx_hash_to_to_address = {
            tx_hash: to_address for tx_hash, to_address in zip(transaction_df["tx_hash"], transaction_df["to_address"])
        }
        rebalance_event_df["solver_address"] = rebalance_event_df["transactionHash"].map(tx_hash_to_to_address)
        new_rebalance_event_rows = add_lp_token_safe_and_spot_prices(rebalance_event_df, autopool)

        new_autopool_state_rows = _fetch_new_autopool_state_rows(autopool, [int(b) for b in transaction_df["block"]])

        insert_avoid_conflicts(new_autopool_state_rows, AutopoolStates)
        insert_avoid_conflicts(new_rebalance_event_rows, RebalanceEvents)


def add_lp_token_safe_and_spot_prices(
    rebalance_event_df: pd.DataFrame,
    autopool: AutopoolConstants,
    max_concurrent_fetches: int = 1,
) -> list[RebalanceEvents]:

    destination_info_df = _load_destination_info_df(autopool)

    destination_vault_address_to_pool = {
        d: p for d, p in zip(destination_info_df["destination_vault_address"], destination_info_df["pool"])
    }

    destination_vault_address_to_pool[autopool.autopool_eth_addr] = autopool.autopool_eth_addr

    rebalance_event_df["poolInAddress"] = rebalance_event_df["destinationInAddress"].map(
        destination_vault_address_to_pool
    )
    rebalance_event_df["poolOutAddress"] = rebalance_event_df["destinationOutAddress"].map(
        destination_vault_address_to_pool
    )

    fetch_semaphore = threading.Semaphore(max_concurrent_fetches)

    def _fetch_prices_and_build_rebalance_event(rebalance_event_row: pd.Series) -> RebalanceEvents:
        with fetch_semaphore:
            safe_value_out, safe_value_in, spot_value_out, spot_value_in = _fetch_values_in_and_out(
                autopool, rebalance_event_row
            )

            spot_value_in_solver_change = _get_spot_value_change_in_solver(
                autopool, destination_info_df, rebalance_event_row
            )

            return RebalanceEvents(
                tx_hash=rebalance_event_row["transactionHash"],
                autopool_vault_address=rebalance_event_row["autopool_vault_address"],
                chain_id=int(rebalance_event_row["chain_id"]),
                rebalance_file_path=rebalance_event_row["rebalance_file_path"],
                destination_out=rebalance_event_row["destinationOutAddress"],
                destination_in=rebalance_event_row["destinationInAddress"],
                quantity_out=float(rebalance_event_row["tokenOutAmount"]),  # not certain this is correct
                quantity_in=float(rebalance_event_row["tokenInAmount"]),  # not certain this is correct, is wrong
                safe_value_out=safe_value_out,
                safe_value_in=safe_value_in,
                spot_value_out=spot_value_out,
                spot_value_in=spot_value_in,
                spot_value_in_solver_change=spot_value_in_solver_change,
            )

    new_rebalance_event_rows: list[RebalanceEvents] = []
    rows = rebalance_event_df.to_dict("records")
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(_fetch_prices_and_build_rebalance_event, row) for row in rows]
        for future in as_completed(futures):
            new_rebalance_event_rows.append(future.result())

    return new_rebalance_event_rows


def _fetch_values_in_and_out(autopool: AutopoolConstants, rebalance_event_row: pd.Series):

    calls = build_lp_token_spot_and_safe_price_calls(
        destination_addresses=[
            rebalance_event_row["destinationInAddress"],
            rebalance_event_row["destinationOutAddress"],
        ],
        lp_token_addresses=[rebalance_event_row["tokenInAddress"], rebalance_event_row["tokenOutAddress"]],
        pool_addresses=[rebalance_event_row["poolInAddress"], rebalance_event_row["poolOutAddress"]],
        chain=autopool.chain,
        base_asset=autopool.base_asset,
    )
    state = get_state_by_one_block(calls, int(rebalance_event_row["blockNumber"]), chain=autopool.chain)

    if (autopool.autopool_eth_addr, "lp_token_spot_and_safe") in state:
        # the vault (idle) safe and spot prices are always 1.0
        state[(autopool.autopool_eth_addr, "lp_token_spot_and_safe")] = (1.0, 1.0)

    token_in_spot_value, token_in_safe_value = state[
        (rebalance_event_row["destinationInAddress"], "lp_token_spot_and_safe")
    ]
    token_out_spot_value, token_out_safe_value = state[
        (rebalance_event_row["destinationOutAddress"], "lp_token_spot_and_safe")
    ]

    safe_value_out = float(token_out_safe_value * rebalance_event_row["tokenOutAmount"])
    safe_value_in = float(token_in_safe_value * rebalance_event_row["tokenInAmount"])

    spot_value_in = float(token_in_spot_value * rebalance_event_row["tokenInAmount"])
    spot_value_out = float(token_out_spot_value * rebalance_event_row["tokenOutAmount"])

    return safe_value_out, safe_value_in, spot_value_out, spot_value_in


def _get_spot_value_change_in_solver(
    autopool: AutopoolConstants, destination_info_df: pd.DataFrame, rebalance_event_row: dict
) -> float:

    def _fetch_non_zero_changes(rebalance_event_row: dict) -> dict:
        tokens_and_decimals = destination_info_df[["token_address", "decimals"]].drop_duplicates()
        balance_of_calls = []

        decimal_normalizer_map = {
            6: safe_normalize_6_with_bool_success,
            18: safe_normalize_with_bool_success,
        }

        for _, token_row in tokens_and_decimals.iterrows():
            token = token_row["token_address"]
            normalizer = decimal_normalizer_map[token_row["decimals"]]
            solver_address = rebalance_event_row["solver_address"]
            call = Call(
                token,
                [
                    "balanceOf(address)(uint256)",
                    solver_address,
                ],
                [(token, normalizer)],
            )
            balance_of_calls.append(call)

        balance_of_df = get_raw_state_by_blocks(
            balance_of_calls,
            [int(rebalance_event_row["blockNumber"]) - 1, int(rebalance_event_row["blockNumber"])],
            chain=autopool.chain,
        )
        diffs = balance_of_df.diff()
        second_row = diffs.iloc[1]
        non_zero_changes = {col: val for col, val in second_row.items() if val != 0}
        return non_zero_changes

    non_zero_changes = _fetch_non_zero_changes(rebalance_event_row)

    out_destination_sub_df = destination_info_df[
        destination_info_df["destination_vault_address"] == rebalance_event_row["destinationOutAddress"]
    ]
    in_destination_sub_df = destination_info_df[
        (destination_info_df["destination_vault_address"] == rebalance_event_row["destinationInAddress"])
        & ~destination_info_df["token_address"].isin(out_destination_sub_df["token_address"])
    ]
    destination_token_in_for_spot_prices = pd.concat([out_destination_sub_df, in_destination_sub_df])

    destination_token_in_for_spot_prices["base_asset"] = autopool.base_asset
    destination_token_in_for_spot_prices["base_asset_decimals"] = autopool.base_asset_decimals

    if autopool.base_asset in WETH:
        spot_price_calls_function = _build_get_spot_price_in_eth_calls
    else:
        spot_price_calls_function = _build_get_spot_price_in_quote_calls

    spot_price_dict = get_state_by_one_block(
        spot_price_calls_function(autopool.chain, destination_token_in_for_spot_prices),
        rebalance_event_row["blockNumber"],
        chain=autopool.chain,
    )

    token_spot_price = {token: value for (pool, token, spot_price_string_id), value in spot_price_dict.items()}
    token_spot_price[autopool.base_asset] = 1.0
    spot_value_difference_in_solver = 0

    for k, token_quantity_change_before_and_after_rebalance in non_zero_changes.items():
        spot_value_difference_in_solver += token_spot_price[k] * token_quantity_change_before_and_after_rebalance

    # positve value means there was extra value left in solver
    # negative value means there was value taken out of the solver
    return spot_value_difference_in_solver


if __name__ == "__main__":

    from mainnet_launch.constants import *

    # 50 seconds, need to make subgrapoh calls in parallel
    # for each autopool
    # thread pool executors
    profile_function(_load_raw_rebalance_event_df, AUTO_ETH)
    # profile_function(_load_raw_rebalance_event_df, BASE_ETH)
