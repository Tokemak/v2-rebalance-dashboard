import json
from concurrent.futures import ThreadPoolExecutor

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
    ensure_all_transactions_are_saved_in_db
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


def _extract_rebalance_event_row(row: pd.Series) -> RebalanceEvents:
    return RebalanceEvents(
        tx_hash=row["transactionHash"],
        autopool_vault_address=row["autopool_vault_address"],
        chain_id=row["chain_id"],
        rebalance_file_path=row["rebalance_file_path"],
        destination_out=row["destinationOutAddress"],
        destination_in=row["destinationInAddress"],
        quantity_out=row["tokenOutAmount"],
        quantity_in=row["tokenInAmount"],
    )


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

        if len(rebalance_event_df) == 0:
            continue

        hash_to_plan = _connect_plans_to_rebalance_evnets(rebalance_event_df, rebalance_plan_df)

        rebalance_event_df["rebalance_file_path"] = rebalance_event_df["transactionHash"].map(hash_to_plan)
        rebalance_event_df["autopool_vault_address"] = autopool.autopool_eth_addr
        rebalance_event_df["chain_id"] = autopool.chain.chain_id

        # ensure_all_transactions_are_saved_in_db(rebalance_event_df['transactionHash'].to_list(), autopool.chain)


        add_lp_token_safe_and_spot_prices(rebalance_event_df, autopool)

        # return 
        # new_rebalance_events_rows = rebalance_event_df.apply(_extract_rebalance_event_row, axis=1).to_list()

        # insert_avoid_conflicts(new_events, RebalanceEvents)
# 

@time_decorator
def add_lp_token_safe_and_spot_prices(rebalance_event_df:pd.DataFrame, autopool:AutopoolConstants):


    destinations_df = get_full_table_as_df(Destinations)

    destination_vault_address_to_pool = {d:p for d, p in  zip(destinations_df['destination_vault_address'], destinations_df['pool'])}

    rebalance_event_df['poolInAddress'] = rebalance_event_df['destinationInAddress'].map(destination_vault_address_to_pool)
    rebalance_event_df['poolOutAddress'] = rebalance_event_df['destinationOutAddress'].map(destination_vault_address_to_pool)

    blocks = [int(b) for b in rebalance_event_df['blockNumber']]

    pass

    # hmm, this is wildly over what is needed way to wide

    token_in_value_calls = build_lp_token_spot_and_safe_price_calls(
        rebalance_event_df['destinationInAddress'], rebalance_event_df['tokenInAddress'], rebalance_event_df['poolInAddress'],
        autopool.chain, autopool.base_asset
    )


    safe_and_spot_token_value_in_df = get_raw_state_by_blocks(
        token_in_value_calls, blocks, chain=autopool.chain, include_block_number=True
    )

    pass


if __name__ == '__main__':
    ensure_rebalance_events_are_updated()