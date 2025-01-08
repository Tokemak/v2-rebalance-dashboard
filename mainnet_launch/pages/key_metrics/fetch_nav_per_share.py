import pandas as pd
from multicall import Call
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
)

from mainnet_launch.constants import (
    ALL_AUTOPOOLS,
    AutopoolConstants,
    ChainData,
    ETH_CHAIN,
    BASE_CHAIN,
)
from mainnet_launch.database.new_databases import (
    write_dataframe_to_table,
    get_earliest_block_from_table_with_chain,
    run_read_only_query,
)
from mainnet_launch.database.should_update_database import should_update_table
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column


NAV_PER_SHARE_TABLE = "NAV_PER_SHARE_TABLE"


def add_new_nav_per_share_to_table():
    for chain in [ETH_CHAIN, BASE_CHAIN]:
        highest_block_already_fetched = get_earliest_block_from_table_with_chain(NAV_PER_SHARE_TABLE, chain)
        blocks = [b for b in build_blocks_to_use(chain) if b >= highest_block_already_fetched]
        nav_per_share_df = _fetch_nav_per_share_from_external_source(chain, blocks)
        write_dataframe_to_table(nav_per_share_df, NAV_PER_SHARE_TABLE)


def _fetch_nav_per_share_from_external_source(chain: ChainData, blocks: list[int]):
    calls = [nav_per_share_call(a.name, a.autopool_eth_addr) for a in ALL_AUTOPOOLS if a.chain == chain]
    nav_per_share_df = get_raw_state_by_blocks(calls=calls, blocks=blocks, chain=chain, include_block_number=True)

    column_names = [a.name for a in ALL_AUTOPOOLS if a.chain == chain]
    long_nav_per_share_df = pd.melt(
        nav_per_share_df, id_vars=["block"], value_vars=column_names, var_name="autopool", value_name="nav_per_share"
    )
    long_nav_per_share_df["chain"] = chain.name
    return long_nav_per_share_df


def nav_per_share_call(name: str, autopool_vault_address: str) -> Call:
    return Call(
        autopool_vault_address,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [(name, safe_normalize_with_bool_success)],
    )


def fetch_nav_per_share(autopool: AutopoolConstants) -> pd.DataFrame:
    if should_update_table(NAV_PER_SHARE_TABLE):
        add_new_nav_per_share_to_table()

    query = f"""
        SELECT * from {NAV_PER_SHARE_TABLE}
        WHERE autopool = ?
        """
    params = (autopool.name,)
    long_nav_per_share_df = run_read_only_query(query, params)
    nav_per_share_df = long_nav_per_share_df.pivot(
        index="block", columns="autopool", values="nav_per_share"
    ).reset_index()
    nav_per_share_df = add_timestamp_to_df_with_block_column(nav_per_share_df, autopool.chain)

    # nav_per_share_df = _fetch_all_all_pool_nav_per_share(blocks)[[autopool.name]]
    nav_per_share_df = nav_per_share_df.resample("1D").last()

    # Calculate the 30-day difference and annualized return
    nav_per_share_df["30_day_difference"] = nav_per_share_df[autopool.name].diff(periods=30)
    # Normalized to starting NAV per share for 30-day return
    nav_per_share_df["30_day_annualized_return"] = (
        (nav_per_share_df["30_day_difference"] / nav_per_share_df[autopool.name].shift(30)) * (365 / 30) * 100
    )

    # Calculate the 7-day difference and annualized return
    nav_per_share_df["7_day_difference"] = nav_per_share_df[autopool.name].diff(periods=7)
    # Normalized to starting NAV per share for 7-day return
    nav_per_share_df["7_day_annualized_return"] = (
        (nav_per_share_df["7_day_difference"] / nav_per_share_df[autopool.name].shift(7)) * (365 / 7) * 100
    )

    # Calculate daily returns
    nav_per_share_df["daily_return"] = nav_per_share_df[autopool.name].pct_change()

    # Calculate 7-day moving average of daily returns
    nav_per_share_df["7_day_MA_return"] = nav_per_share_df["daily_return"].rolling(window=7).mean()

    # Annualize the 7-day moving average return
    nav_per_share_df["7_day_MA_annualized_return"] = nav_per_share_df["7_day_MA_return"] * 365 * 100

    # Calculate 30-day moving average of daily returns
    nav_per_share_df["30_day_MA_return"] = nav_per_share_df["daily_return"].rolling(window=30).mean()

    # Annualize the 30-day moving average return
    nav_per_share_df["30_day_MA_annualized_return"] = nav_per_share_df["30_day_MA_return"] * 365 * 100

    return nav_per_share_df


if __name__ == "__main__":
    nav_per_share_df = fetch_nav_per_share(ALL_AUTOPOOLS[2])
    print(nav_per_share_df.head())
    print(nav_per_share_df.tail())

    nav_per_share_df = fetch_nav_per_share(ALL_AUTOPOOLS[1])
    print(nav_per_share_df.head())
    print(nav_per_share_df.tail())
