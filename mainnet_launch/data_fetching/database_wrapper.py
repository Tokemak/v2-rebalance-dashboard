# def generic wrapper
import pandas as pd
from mainnet_launch.constants import ChainData, ETH_CHAIN, ChainData
from mainnet_launch.data_fetching.should_update_database import should_update_table
from mainnet_launch.data_fetching.new_databases import load_table, write_dataframe_to_table


def mock_data_fetching_function(min_block: int, chain: ChainData) -> pd.DataFrame:

    df = pd.DataFrame()
    df["block"] = [19_000_000, 20_000_000]
    df["A"] = [1, 1]

    # for the min timestamp, get the first highest block where the timestamp is less than min_timestamp and the chain is the chain
    return df


# untested
def update_table_if_needed(
    table_name: str,
    max_latency: str,
    chain: ChainData,
    fetch_data_from_external_source_function,
    get_block_to_use_function,
):
    """Calling this garentuees that the table on disk is updated and ready to use"""
    # assumes the only input is block and chain

    if should_update_table(table_name, max_latency):
        block_to_use = get_block_to_use_function()
        possible_new_rows_df = fetch_data_from_external_source_function(block_to_use, chain)
        if not isinstance(possible_new_rows_df, pd.DataFrame):
            raise ValueError("expected possible_new_rows_df to be a pd.DataFrame but was ", type(possible_new_rows_df))
        write_dataframe_to_table(possible_new_rows_df, table_name)

    if should_update_table(table_name, max_latency):
        raise ValueError("just updated table, should not want to update table again")
