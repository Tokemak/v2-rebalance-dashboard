import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import plotly.express as px
from mainnet_launch.constants import (
    AutopoolConstants,
    STATS_CALCULATOR_REGISTRY,
    ALL_CHAINS,
    ROOT_PRICE_ORACLE,
    ChainData,
    ETH_CHAIN,
)
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
from mainnet_launch.abis import AUTOPOOL_VAULT_ABI


from mainnet_launch.database.database_operations import (
    write_dataframe_to_table,
    get_earliest_block_from_table_with_chain,
    get_all_rows_in_table_by_chain,
)
from mainnet_launch.database.should_update_database import should_update_table

from mainnet_launch.abis import STATS_CALCULATOR_REGISTRY_ABI


from multicall import Call
from mainnet_launch.data_fetching.get_state_by_block import (
    identity_with_bool_success,
    get_raw_state_by_blocks,
    get_state_by_one_block,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
)


ASSET_BACKING_AND_PRICES = "ASSET_BACKING_AND_PRICES"


def add_new_asset_oracle_and_discount_price_rows_to_table():
    if should_update_table(ASSET_BACKING_AND_PRICES):
        for chain in ALL_CHAINS:
            # must add BASE as well purely for the support
            highest_block_already_fetched = get_earliest_block_from_table_with_chain(ASSET_BACKING_AND_PRICES, chain)
            asset_oracle_and_backing_df = _fetch_backing_and_oracle_price_df_from_external_source(
                chain, highest_block_already_fetched
            )
            asset_oracle_and_backing_df["chain"] = chain.name
            write_dataframe_to_table(asset_oracle_and_backing_df, ASSET_BACKING_AND_PRICES)


def _fetch_lst_calc_addresses_df(chain: ChainData) -> pd.DataFrame:
    # returns a dataframe of the LST address, LST.symbol, and LST calculator
    # 3 total http calls total, fine not to have a table for htis
    stats_calculator_registry_contract = chain.client.eth.contract(
        STATS_CALCULATOR_REGISTRY(chain), abi=STATS_CALCULATOR_REGISTRY_ABI
    )

    StatCalculatorRegistered = fetch_events(stats_calculator_registry_contract.events.StatCalculatorRegistered)

    lstTokenAddress_calls = [
        Call(
            a,
            ["lstTokenAddress()(address)"],
            [(a, identity_with_bool_success)],
        )
        for a in StatCalculatorRegistered["calculatorAddress"]
    ]

    calculator_to_lst_address = get_state_by_one_block(
        lstTokenAddress_calls, chain.client.eth.block_number, chain=chain
    )
    StatCalculatorRegistered["lst"] = StatCalculatorRegistered["calculatorAddress"].map(calculator_to_lst_address)
    lst_calcs = StatCalculatorRegistered[~StatCalculatorRegistered["lst"].isna()].copy()

    symbol_calls = [
        Call(
            a,
            ["symbol()(string)"],
            [(a, identity_with_bool_success)],
        )
        for a in lst_calcs["lst"]
    ]
    calculator_to_lst_address = get_state_by_one_block(symbol_calls, chain.client.eth.block_number, chain=chain)
    lst_calcs["symbol"] = lst_calcs["lst"].map(calculator_to_lst_address)

    return lst_calcs[["lst", "symbol", "calculatorAddress"]]


def _fetch_backing_and_oracle_price_df_from_external_source(chain: ChainData, start_block: int) -> pd.DataFrame:

    lst_calcs = _fetch_lst_calc_addresses_df(chain)

    token_symbols_to_ignore = ["OETH", "stETH", "eETH"]
    # skip stETH and eETH because they are captured in wstETH and weETH
    # skip OETH because we dropped it in October 2024,
    lst_calcs = lst_calcs[~lst_calcs["symbol"].isin(token_symbols_to_ignore)].copy()

    oracle_price_calls = [
        Call(
            ROOT_PRICE_ORACLE(chain),
            ["getPriceInEth(address)(uint256)", lst],
            [(f"{symbol}_oracle_price", safe_normalize_with_bool_success)],
        )
        for (lst, symbol) in zip(lst_calcs["lst"], lst_calcs["symbol"])
    ]

    backing_calls = [
        Call(
            calculatorAddress,
            ["calculateEthPerToken()(uint256)"],
            [(f"{symbol}_backing", safe_normalize_with_bool_success)],
        )
        for (calculatorAddress, symbol) in zip(lst_calcs["calculatorAddress"], lst_calcs["symbol"])
    ]

    blocks = build_blocks_to_use(chain, start_block=start_block)
    wide_oracle_and_backing_df = get_raw_state_by_blocks(
        [*oracle_price_calls, *backing_calls], blocks, chain, include_block_number=True
    )

    long_oracle_and_backing_df = _raw_price_and_backing_data_to_long_format(wide_oracle_and_backing_df)
    long_oracle_and_backing_df["percent_discount"] = 100 - (
        100 * long_oracle_and_backing_df["oracle_price"] / long_oracle_and_backing_df["backing"]
    )

    return long_oracle_and_backing_df


def _raw_price_and_backing_data_to_long_format(wide_df: pd.DataFrame) -> pd.DataFrame:
    # takes the wide dataframe and makes it long instead
    # this is so that new LSTs can be added without breaking the table

    # timestamp	                    symbol	backing	    oracle_price
    # 0	2024-09-15 02:04:47+00:00	cbETH	1.081472	1.079332
    # 1	2024-09-15 08:06:23+00:00	cbETH	1.081472	1.079332

    backing_df = wide_df[[c for c in wide_df.columns if (("_backing" in c) or (c == "block"))]]
    long_backing_df = backing_df.melt(id_vars=["block"], var_name="symbol", value_name="backing")
    long_backing_df["symbol"] = long_backing_df["symbol"].apply(lambda x: str(x).replace("_backing", ""))

    oracle_price_df = wide_df[[c for c in wide_df.columns if (("_oracle_price" in c) or (c == "block"))]]
    long_oracle_price_df = oracle_price_df.melt(id_vars=["block"], var_name="symbol", value_name="oracle_price")
    long_oracle_price_df["symbol"] = long_oracle_price_df["symbol"].apply(lambda x: str(x).replace("_oracle_price", ""))

    long_df = pd.merge(long_backing_df, long_oracle_price_df, on=["block", "symbol"])
    return long_df


def _extract_backing_price_and_percent_discount_dfs(
    long_df: pd.DataFrame, autopoool: AutopoolConstants
) -> pd.DataFrame:
    # convert the long_df (as it is stored on disk) and converts it back into the wide format
    # to use elsewhere
    # timestamp	cbETH_backing	cbETH_oracle_price
    # 2024-09-15 02:04:47+00:00	1.081472	1.079332
    # 2024-09-15 08:06:23+00:00	1.081472	1.079332

    # this does not maintain column order

    long_df = add_timestamp_to_df_with_block_column(long_df, autopoool.chain).reset_index()

    long_df["percent_discount"] = 100 - (100 * long_df["oracle_price"] / long_df["backing"])

    wide_backing_df = long_df[["timestamp", "symbol", "backing"]].pivot(
        index="timestamp", columns="symbol", values="backing"
    )
    wide_oracle_price_df = long_df[["timestamp", "symbol", "oracle_price"]].pivot(
        index="timestamp", columns="symbol", values="oracle_price"
    )
    wide_percent_discount_df = long_df[["timestamp", "symbol", "percent_discount"]].pivot(
        index="timestamp", columns="symbol", values="percent_discount"
    )

    wide_backing_df["WETH"] = 1
    wide_oracle_price_df["WETH"] = 1
    wide_percent_discount_df["WETH"] = 0

    wide_backing_df["stETH"] = 1  # the definition of stETH
    wide_oracle_price_df["WETH"] = 1
    wide_oracle_price_df["stETH"] = wide_oracle_price_df["wstETH"] / wide_backing_df["wstETH"]
    wide_percent_discount_df["WETH"] = 0
    wide_percent_discount_df["stETH"] = wide_percent_discount_df["wstETH"]

    return wide_backing_df, wide_oracle_price_df, wide_percent_discount_df


def fetch_and_render_asset_oracle_and_backing():
    add_new_asset_oracle_and_discount_price_rows_to_table()
    long_asset_oracle_and_backing_df = get_all_rows_in_table_by_chain(ASSET_BACKING_AND_PRICES, ETH_CHAIN)
    # by default only show the price and discount data for Ethereum

    wide_backing_df, wide_oracle_price_df, wide_percent_discount_df = _extract_backing_price_and_percent_discount_dfs(
        long_asset_oracle_and_backing_df
    )

    backing_figure = px.line(wide_backing_df, title="Backing", labels={"index": "Date", "value": "ETH"})
    oracle_price_figure = px.line(wide_oracle_price_df, title="Oracle Price", labels={"index": "Date", "value": "ETH"})
    percent_discount_figure = px.line(
        wide_percent_discount_df, title="Percent Discount", labels={"index": "Date", "value": "Percent"}
    )

    st.header("Asset Backing, Oracle Price and Percent Discount")
    st.plotly_chart(percent_discount_figure, use_container_width=True)
    st.plotly_chart(oracle_price_figure, use_container_width=True)
    st.plotly_chart(backing_figure, use_container_width=True)

    with st.expander("Explanation"):
        st.markdown(
            """
        ### Key Terms:
        - **Oracle Price**: The "safe" value of an asset based on `RootPriceOracle.getPriceInEth()`.
        - **Backing**: The underlying value of the asset on the consensus layer, based on `lstCalculator.calculateEthPerToken()`.
        - **Percent Discount**: Calculated as:  
        `100 - (100 * Oracle Price / Backing)`

        ### Examples:

        **Asset Trades at a Discount**  
        - pxETH Oracle Price = `0.95`  
        - pxETH Backing = `1`  
        - Percent Discount = `100 - (100 * 0.95 / 1)` = **5% discount**

        **Asset Trades at a Premium** 
        - pxETH Oracle Price = `1.05`  
        - pxETH Backing = `1`  
        - Percent Discount = `100 - (100 * 1.05 / 1)` = **-5% discount** = **5% premium**

        ---
        """
        )


if __name__ == "__main__":
    fetch_and_render_asset_oracle_and_backing()
