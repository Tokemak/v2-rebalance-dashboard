import pandas as pd
import streamlit as st
from multicall import Call
import plotly.express as px
import plotly.subplots as sp
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    get_state_by_one_block,
    identity_with_bool_success,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
)
from mainnet_launch.data_fetching.databases import write_df_to_table, load_table_if_exists

from mainnet_launch.data_fetching.get_events import fetch_events

from mainnet_launch.constants import (
    INCENTIVE_PRICNIG_STATS,
    LIQUIDATION_ROW,
    eth_client,
    CACHE_TIME,
    ROOT_PRICE_ORACLE,
    AutopoolConstants,
    ChainData,
    ETH_CHAIN,
    BASE_CHAIN,
    time_decorator,
)

from mainnet_launch.abis.abis import DESTINATION_DEBT_REPORTING_SWAPPED_ABI


INCENTIVE_TOKEN_PRICES_TABLE_NAME = "INCENTIVE_TOKEN_PRICES_AT_LIQUIDATION"


def _add_acheived_price_column(swapped_df: pd.DataFrame, token_address_to_decimals: dict):
    def _compute_achieved_price(row):
        sell_token_decimals = token_address_to_decimals[row["sellTokenAddress"]]
        normalized_sell_amount = row["sellAmount"] / (10**sell_token_decimals)

        buy_token_decimals = token_address_to_decimals[row["buyTokenAddress"]]
        normalized_buy_amount = row["buyTokenAmountReceived"] / (10**buy_token_decimals)

        achieved_price = normalized_buy_amount / normalized_sell_amount
        return achieved_price

    swapped_df["achieved_price"] = swapped_df.apply(lambda row: _compute_achieved_price(row), axis=1)

    return swapped_df


def _fetch_oracle_price_df(swapped_df: pd.DataFrame, chain: ChainData) -> pd.DataFrame:
    token_addresses = [*swapped_df["sellTokenAddress"].unique(), *swapped_df["buyTokenAddress"].unique()]
    oracle_price_calls = [
        Call(
            ROOT_PRICE_ORACLE(chain),
            ["getPriceInEth(address)(uint256)", addr],
            [(addr, safe_normalize_with_bool_success)],
        )
        for addr in token_addresses
    ]

    blocks = swapped_df["block"].unique()
    oracle_price_df = get_raw_state_by_blocks(oracle_price_calls, blocks, chain, include_block_number=True)
    return oracle_price_df


def _fetch_incentive_calculator_price_df(swapped_df: pd.DataFrame, chain: ChainData) -> pd.DataFrame:
    token_addresses = [*swapped_df["sellTokenAddress"].unique(), *swapped_df["buyTokenAddress"].unique()]

    def _min_of_low_and_high_price(success, data):
        if success:
            fast, slow = data
            return min(fast, slow) / 1e18
        return None

    oracle_price_calls = [
        Call(
            INCENTIVE_PRICNIG_STATS(chain),
            ["getPrice(address,uint40)((uint256,uint256))", addr, 2 * 86400],  # 2 days of latency
            [(addr, _min_of_low_and_high_price)],
        )
        for addr in token_addresses
    ]

    blocks = swapped_df["block"].unique()
    incentive_calculator_price_df = get_raw_state_by_blocks(
        oracle_price_calls, blocks, chain, include_block_number=True
    )
    return incentive_calculator_price_df


def _fetch_reward_token_price_during_liquidation(chain: ChainData, start_block: int) -> pd.DataFrame:
    contract = chain.client.eth.contract(LIQUIDATION_ROW(chain), abi=DESTINATION_DEBT_REPORTING_SWAPPED_ABI)

    swapped_df = fetch_events(contract.events.Swapped, start_block=start_block)
    oracle_price_df = _fetch_oracle_price_df(swapped_df, chain)
    incentive_calculator_price_df = _fetch_incentive_calculator_price_df(swapped_df, chain)

    token_addresses = [*swapped_df["sellTokenAddress"].unique(), *swapped_df["buyTokenAddress"].unique()]
    symbol_calls = [Call(addr, ["symbol()(string)"], [(addr, identity_with_bool_success)]) for addr in token_addresses]
    token_address_to_symbol = get_state_by_one_block(symbol_calls, swapped_df["block"].max(), chain)

    decimals_calls = [
        Call(addr, ["decimals()(uint8)"], [(addr, identity_with_bool_success)]) for addr in token_addresses
    ]

    token_address_to_decimals = get_state_by_one_block(decimals_calls, swapped_df["block"].max(), chain)

    swapped_df = _add_acheived_price_column(swapped_df, token_address_to_decimals)

    long_oracle_prices_df = pd.melt(
        oracle_price_df,
        id_vars=["block"],
        var_name="sellTokenAddress",
        value_name="oracle_price",
    )
    long_swapped_df = swapped_df[["block", "sellTokenAddress", "achieved_price"]].copy()

    long_incentive_calculator_prices = pd.melt(
        incentive_calculator_price_df,
        id_vars=["block"],
        var_name="sellTokenAddress",
        value_name="incentive_calculator_price",
    )

    long_swapped_df = swapped_df[["block", "sellTokenAddress", "achieved_price"]].copy()
    long_swapped_df = pd.merge(long_swapped_df, long_oracle_prices_df, on=["block", "sellTokenAddress"], how="left")
    long_swapped_df = pd.merge(
        long_swapped_df, long_incentive_calculator_prices, on=["block", "sellTokenAddress"], how="left"
    )

    timestamp_df = oracle_price_df.reset_index()[["timestamp", "block"]]

    long_swapped_df = pd.merge(long_swapped_df, timestamp_df, on=["block"], how="left")
    long_swapped_df["tokenSymbol"] = long_swapped_df["sellTokenAddress"].apply(lambda x: token_address_to_symbol[x])
    long_swapped_df["chain"] = chain.name

    return long_swapped_df


def fetch_and_render_reward_token_achieved_vs_incentive_token_price():
    fetch_reward_token_achieved_vs_incentive_token_price()
    swapped_df = load_table_if_exists(INCENTIVE_TOKEN_PRICES_TABLE_NAME)
    if swapped_df is None:
        raise ValueError('Failed to read swapped df from disk because the table does not exist')
    
    today = datetime.now(timezone.utc)
    thirty_days_ago = today - timedelta(days=30)

    for chainName in [ETH_CHAIN.name, BASE_CHAIN.name]:
        # do some pivot tables here
        incentive_stats_token_prices_df, oracle_price_df, achieved_eth_price_df = chain_to_dfs[chainName]

        st.header(f"{chainName} Achieved vs Expected Token Price")

        st.plotly_chart(
            _make_histogram_of_percent_diff(
                achieved_eth_price_df[achieved_eth_price_df.index > thirty_days_ago],
                incentive_stats_token_prices_df[incentive_stats_token_prices_df.index > thirty_days_ago],
                "Previous 30 Days: Percent Difference Achieved vs Incentive Stats Price",
            ),
            use_container_width=True,
        )

        st.plotly_chart(
            _make_histogram_of_percent_diff(
                achieved_eth_price_df[achieved_eth_price_df.index > thirty_days_ago],
                oracle_price_df[oracle_price_df.index > thirty_days_ago],
                "Previous 30 Days: Percent Difference Achieved vs Oracle Price",
            ),
            use_container_width=True,
        )

        st.plotly_chart(
            _make_histogram_of_percent_diff(
                achieved_eth_price_df,
                incentive_stats_token_prices_df,
                "Since Inception: Percent Difference Achieved vs Incentive Stats Price",
            ),
            use_container_width=True,
        )

        st.plotly_chart(
            _make_histogram_of_percent_diff(
                achieved_eth_price_df, oracle_price_df, "Since Inception: Percent Difference Achieved vs Oracle Price"
            ),
            use_container_width=True,
        )

    with st.expander("Description"):
        st.write(
            """
            ## Achieved Price
            - The actual ratio of tokens sold / WETH when selling reward tokens
            
            ## Achieved vs Incentive Stats Price
            - This metric uses the Incentive Stats contract to obtain the minimum of the fast and slow filtered incentive token prices. 
            - This provides a conservative estimate of the incentive token's value.
            - Positive values indicate that we sold the incentive token for more than the Incentive Stats Price at that block

            ## Achieved vs Oracle Price
            - This metric uses the Root Price Oracle to fetch the current price of the incentive token (typically via Chainlink)
            - Positive values indicate that we sold the incentive token for more than the oracle price at that block.
            """
        )


def _update_swapped_df():
    """Refresh the swapped_df on disk"""
    swapped_df = load_table_if_exists(INCENTIVE_TOKEN_PRICES_TABLE_NAME)
    # I suspect that the USDC prices on base are wrong, TODO use identiy functions and clean them up in memory

    if swapped_df is None:
        highest_eth_block_already_fetched = ETH_CHAIN.block_autopool_first_deployed
        highest_base_block_already_fetched = BASE_CHAIN.block_autopool_first_deployed
    else:
        highest_eth_block_already_fetched = int(swapped_df[swapped_df["chain"] == ETH_CHAIN.name]["block"].max())
        highest_base_block_already_fetched = int(swapped_df[swapped_df["chain"] == BASE_CHAIN.name]["block" ].max())
            
       
    eth_swapped_df = _fetch_reward_token_price_during_liquidation(ETH_CHAIN, highest_eth_block_already_fetched)
    write_df_to_table(eth_swapped_df, INCENTIVE_TOKEN_PRICES_TABLE_NAME)
    
    base_swapped_df = _fetch_reward_token_price_during_liquidation(BASE_CHAIN, highest_base_block_already_fetched)
    write_df_to_table(base_swapped_df, INCENTIVE_TOKEN_PRICES_TABLE_NAME)
    


def fetch_reward_token_achieved_vs_incentive_token_price():
    _update_swapped_df()


def _make_histogram_of_percent_diff(
    incentive_token_prices_df: pd.DataFrame, achieved_eth_price_df: pd.DataFrame, title: str
):
    percent_diff = 100 * ((incentive_token_prices_df - achieved_eth_price_df) / incentive_token_prices_df)

    num_columns = int(len(percent_diff.columns) / 3) + 1
    num_rows = int(len(percent_diff.columns) / 3) + 1
    fig = sp.make_subplots(
        rows=num_rows,
        cols=num_columns,
        subplot_titles=percent_diff.columns,
        x_title="Percent Difference Achieved vs Expected",
        y_title="Percent of Reward Liqudations",
    )

    # makes the histograms have the same scale
    all_data = percent_diff.fillna(0).values.flatten()
    bin_range = [all_data.min(), all_data.max()]
    bin_range = [int(bin_range[0]) - 1, int(bin_range[1]) + 1]
    bin_width = 1

    for i, column in enumerate(percent_diff.columns):
        row = (i // num_columns) + 1
        col = (i % num_columns) + 1

        hist = go.Histogram(
            histnorm="percent", x=percent_diff[column], xbins=dict(start=bin_range[0], end=bin_range[1], size=bin_width)
        )

        fig.add_trace(hist, row=row, col=col)
        fig.update_xaxes(range=bin_range, row=row, col=col, autorange=False)

    fig.update_layout(height=600, width=900, showlegend=False, title=title)
    return fig


if __name__ == "__main__":
    fetch_and_render_reward_token_achieved_vs_incentive_token_price()
