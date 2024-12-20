import pandas as pd

from mainnet_launch.constants import ALL_AUTOPOOLS, ETH_CHAIN, BASE_CHAIN, AutopoolConstants
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI

from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
from mainnet_launch.data_fetching.new_databases import (
    write_dataframe_to_table,
    load_table,
    run_read_only_query,
    does_table_exist,
)
from mainnet_launch.data_fetching.should_update_database import should_update_table


FEE_EVENTS_TABLE = "FEE_EVENTS_TABLE"


def fetch_fee_events_from_autopool(autopool: AutopoolConstants, start_block: int) -> pd.DataFrame:
    vault_contract = autopool.chain.client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
    streaming_fee_df = fetch_events(vault_contract.events.FeeCollected, start_block)
    periodic_fee_df = fetch_events(vault_contract.events.PeriodicFeeCollected, start_block)
    cols = ["event", "block", "hash", "normalized_fees"]
    fee_df = pd.concat([periodic_fee_df, streaming_fee_df], axis=0)
    fee_df["normalized_fees"] = fee_df["fees"].apply(lambda x: int(x) / 1e18)
    try:
        fee_df = fee_df[cols].copy()
    except Exception as e:
        print(fee_df.columns)
        pass
    fee_df["autopool"] = autopool.name
    fee_df["chain"] = autopool.chain.name
    fee_df = add_timestamp_to_df_with_block_column(fee_df, autopool.chain).reset_index()
    return fee_df


def fetch_fee_events_since_start_block(eth_start_block: int, base_start_block: int) -> pd.DataFrame:
    """
    Fetch all the the fees in ETH from the feeCollected and PeriodicFeeCollected events for each autopool
    """

    fee_dfs = []
    for autopool in ALL_AUTOPOOLS:
        if autopool.chain == ETH_CHAIN:
            start_block = eth_start_block
        elif autopool.chain == ETH_CHAIN:
            start_block = base_start_block

        autopool_fee_df = fetch_fee_events_from_autopool(autopool, start_block)
        fee_dfs.append(autopool_fee_df)
    fee_df = pd.concat(fee_dfs)
    fee_df.sort_index(inplace=True)
    return fee_df


def _should_update_fee_table():
    if not does_table_exist(FEE_EVENTS_TABLE):
        return True
    else:
        return should_update_table(FEE_EVENTS_TABLE)


def fetch_fee_df():
    if _should_update_fee_table():
        if not does_table_exist(FEE_EVENTS_TABLE):
            eth_start_block = ETH_CHAIN.block_autopool_first_deployed
            base_start_block = BASE_CHAIN.block_autopool_first_deployed
        else:
            query = f"SELECT MAX(block) AS highest_block FROM {FEE_EVENTS_TABLE} WHERE chain = ?;"
            eth_start_block = run_read_only_query(query, [ETH_CHAIN.name]).loc[0, "highest_block"]
            base_start_block = run_read_only_query(query, [BASE_CHAIN.name]).loc[0, "highest_block"]

        fee_events_df = fetch_fee_events_since_start_block(eth_start_block, base_start_block)
        write_dataframe_to_table(fee_events_df, FEE_EVENTS_TABLE)
    else:
        print("fee_events_df was updated recently, not fetching external data")

    fee_events_df = load_table(FEE_EVENTS_TABLE)
    return fee_events_df


if __name__ == "__main__":
    fetch_fee_df()
