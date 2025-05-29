import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import pandas as pd
from web3 import Web3

from mainnet_launch.database.schema.full import (
    RebalancePlans,
    Destinations,
    DexSwapSteps,
    Tokens,
    RebalanceEvents,
)
from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
    get_full_table_as_df,
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
)
from mainnet_launch.data_fetching.get_state_by_block import get_raw_state_by_blocks, get_state_by_one_block
from mainnet_launch.data_fetching.tokemak_subgraph import fetch_autopool_rebalance_events_from_subgraph
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
)
from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants, time_decorator


from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_destinations_states_table import (
    build_lp_token_spot_and_safe_price_calls,
)


def _connect_plans_to_rebalance_evnets(
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

@time_decorator
def ensure_rebalance_events_are_updated():
    for autopool in ALL_AUTOPOOLS:

        rebalance_event_df = fetch_autopool_rebalance_events_from_subgraph(autopool)
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

        if rebalance_event_df.empty:
            print(autopool.name, "no new rebalance events to fetch")
            continue

        hash_to_plan = _connect_plans_to_rebalance_evnets(rebalance_event_df, rebalance_plan_df)

        rebalance_event_df["rebalance_file_path"] = rebalance_event_df["transactionHash"].map(hash_to_plan)
        rebalance_event_df["autopool_vault_address"] = autopool.autopool_eth_addr
        rebalance_event_df["chain_id"] = autopool.chain.chain_id

        ensure_all_transactions_are_saved_in_db(rebalance_event_df["transactionHash"].to_list(), autopool.chain)

        new_rebalance_event_rows = add_lp_token_safe_and_spot_prices(rebalance_event_df, autopool)
        insert_avoid_conflicts(new_rebalance_event_rows, RebalanceEvents)


def add_lp_token_safe_and_spot_prices(
    rebalance_event_df: pd.DataFrame,
    autopool: AutopoolConstants,
    max_concurrent_fetches: int = 50,
) -> list[RebalanceEvents]:
    destinations_df = get_full_table_as_df(Destinations)
    destination_vault_address_to_pool = {
        d: p for d, p in zip(destinations_df["destination_vault_address"], destinations_df["pool"])
    }

    rebalance_event_df["poolInAddress"] = rebalance_event_df["destinationInAddress"].map(
        destination_vault_address_to_pool
    )
    rebalance_event_df["poolOutAddress"] = rebalance_event_df["destinationOutAddress"].map(
        destination_vault_address_to_pool
    )

    fetch_semaphore = threading.Semaphore(max_concurrent_fetches)

    def _fetch_prices_and_build_rebalance_event(row: pd.Series) -> RebalanceEvents:
        with fetch_semaphore:

            calls = build_lp_token_spot_and_safe_price_calls(
                destination_addresses=[row["destinationInAddress"], row["destinationOutAddress"]],
                lp_token_addresses=[row["tokenInAddress"], row["tokenOutAddress"]],
                pool_addresses=[row["poolInAddress"], row["poolOutAddress"]],
                chain=autopool.chain,
                base_asset=autopool.base_asset,
            )

            state = get_state_by_one_block(calls, int(row["blockNumber"]), chain=autopool.chain)

            if (autopool.autopool_eth_addr, "lp_token_spot_and_safe") in state:
                # the vault safe and spot prices are always 1.0
                state[(autopool.autopool_eth_addr, "lp_token_spot_and_safe")] = (1.0, 1.0)

            token_in_spot_value, token_in_safe_value = state[(row["destinationInAddress"], "lp_token_spot_and_safe")]
            token_out_spot_value, token_out_safe_value = state[(row["destinationOutAddress"], "lp_token_spot_and_safe")]

            safe_value_out = float(token_out_safe_value * row["tokenOutAmount"])
            safe_value_in = float(token_in_safe_value * row["tokenInAmount"])

            spot_value_in = float(token_in_spot_value * row["tokenInAmount"])
            spot_value_out = float(token_out_spot_value * row["tokenOutAmount"])

            swap_offset_period = int(row["swapOffsetPeriod"]) if pd.notna(row["swapOffsetPeriod"]) else None

            return RebalanceEvents(
                tx_hash=row["transactionHash"],
                autopool_vault_address=row["autopool_vault_address"],
                chain_id=int(row["chain_id"]),
                rebalance_file_path=row["rebalance_file_path"],
                destination_out=row["destinationOutAddress"],
                destination_in=row["destinationInAddress"],
                quantity_out=float(row["tokenOutAmount"]),
                quantity_in=float(row["tokenInAmount"]),
                safe_value_out=safe_value_out,
                safe_value_in=safe_value_in,
                spot_value_in=spot_value_in,
                spot_value_out=spot_value_out,
                swap_offset_period=swap_offset_period,
            )

    new_rebalance_event_rows: list[RebalanceEvents] = []
    with ThreadPoolExecutor(max_workers=max_concurrent_fetches) as executor:
        future_to_idx = {
            executor.submit(_fetch_prices_and_build_rebalance_event, row): idx
            for idx, row in rebalance_event_df.iterrows()
        }
        for future in as_completed(future_to_idx):
            new_rebalance_event_rows.append(future.result())

    return new_rebalance_event_rows


if __name__ == "__main__":
    ensure_rebalance_events_are_updated()
