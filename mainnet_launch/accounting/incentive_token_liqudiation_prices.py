import pandas as pd
import streamlit as st
from multicall import Call
import plotly.express as px
import plotly.subplots as sp
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone

import json


from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    get_state_by_one_block,
    identity_with_bool_success,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
)
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column

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
)


def fetch_and_render_reward_token_achieved_vs_incentive_token_price():
    chain_to_dfs = fetch_reward_token_achieved_vs_incentive_token_price()
    today = datetime.now(timezone.utc)
    thirty_days_ago = today - timedelta(days=30)

    for chainName in [ETH_CHAIN.name, BASE_CHAIN.name]:
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


@st.cache_data(ttl=CACHE_TIME)
def fetch_reward_token_achieved_vs_incentive_token_price():

    chain_to_dfs = {}

    for chain in [ETH_CHAIN, BASE_CHAIN]:
        swapped_df = _build_swapped_df(chain)
        incentive_stats_token_prices_df, oracle_price_df, achieved_eth_price_df = _fetch_incentive_token_price_df(
            swapped_df, chain
        )
        chain_to_dfs[chain.name] = (incentive_stats_token_prices_df, oracle_price_df, achieved_eth_price_df)

    return chain_to_dfs


def _build_swapped_df(chain: ChainData):
    # This ABI is ABI is not on etherscan, you have to get it from the v2-core repo foundry contracts when it makes the ABIs
    # fmt: off
    SWAPPED_EVENT_ABI = json.loads(
        """[{"inputs":[{"internalType":"address","name":"aggregator","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[{"internalType":"uint256","name":"balanceNeeded","type":"uint256"},{"internalType":"uint256","name":"balanceAvailable","type":"uint256"}],"type":"error","name":"InsufficientBalance"},{"inputs":[],"type":"error","name":"InsufficientBuyAmount"},{"inputs":[{"internalType":"uint256","name":"buyTokenAmountReceived","type":"uint256"},{"internalType":"uint256","name":"buyAmount","type":"uint256"}],"type":"error","name":"InsufficientBuyAmountReceived"},{"inputs":[],"type":"error","name":"InsufficientSellAmount"},{"inputs":[],"type":"error","name":"SwapFailed"},{"inputs":[],"type":"error","name":"TokenAddressZero"},{"inputs":[{"internalType":"address","name":"sellTokenAddress","type":"address","indexed":true},{"internalType":"address","name":"buyTokenAddress","type":"address","indexed":true},{"internalType":"uint256","name":"sellAmount","type":"uint256","indexed":false},{"internalType":"uint256","name":"buyAmount","type":"uint256","indexed":false},{"internalType":"uint256","name":"buyTokenAmountReceived","type":"uint256","indexed":false}],"type":"event","name":"Swapped","anonymous":false},{"inputs":[],"stateMutability":"view","type":"function","name":"AGGREGATOR","outputs":[{"internalType":"address","name":"","type":"address"}]},{"inputs":[{"internalType":"struct SwapParams","name":"swapParams","type":"tuple","components":[{"internalType":"address","name":"sellTokenAddress","type":"address"},{"internalType":"uint256","name":"sellAmount","type":"uint256"},{"internalType":"address","name":"buyTokenAddress","type":"address"},{"internalType":"uint256","name":"buyAmount","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"},{"internalType":"bytes","name":"extraData","type":"bytes"},{"internalType":"uint256","name":"deadline","type":"uint256"}]}],"stateMutability":"nonpayable","type":"function","name":"swap","outputs":[{"internalType":"uint256","name":"buyTokenAmountReceived","type":"uint256"}]}]"""
    )
    # fmt: on
    contract = chain.client.eth.contract(LIQUIDATION_ROW(chain), abi=SWAPPED_EVENT_ABI)
    swapped_df = fetch_events(contract.events.Swapped)

    token_addresses = list(swapped_df["sellTokenAddress"].unique())

    decimals_calls = [
        Call(
            a,
            ["decimals()(uint8)"],
            [(a, identity_with_bool_success)],
        )
        for a in token_addresses
    ]

    # TODO have a more consistant way to pick a block here
    address_to_decimals = get_state_by_one_block(decimals_calls, chain.client.eth.block_number, chain)

    swapped_df = add_timestamp_to_df_with_block_column(swapped_df, chain)

    def _get_achieved_price(row):
        decimals = address_to_decimals[row["sellTokenAddress"]]
        normalized_sell_amount = row["sellAmount"] / (10**decimals)
        normalized_buy_amount = row["buyTokenAmountReceived"] / 1e18  # always weth so always 1e18

        return normalized_buy_amount / normalized_sell_amount

    swapped_df["achieved_token_price_in_eth"] = swapped_df.apply(lambda row: _get_achieved_price(row), axis=1)
    swapped_df["date"] = swapped_df.index
    return swapped_df


def _fetch_incentive_token_price_df(swapped_df: pd.DataFrame, chain: ChainData):
    symbol_calls = [
        Call(addr, ["symbol()(string)"], [(addr, identity_with_bool_success)])
        for addr in swapped_df["sellTokenAddress"].unique()
    ]
    block = max(swapped_df["block"])
    token_address_to_symbol = get_state_by_one_block(symbol_calls, block, chain=chain)

    def _min_of_low_and_high_price(success, data):
        if success:
            fast, slow = data
            return min(fast, slow) / 1e18
        return None

    def getIncentiveTokenPrice(name: str, token_address: str) -> Call:
        return Call(
            INCENTIVE_PRICNIG_STATS(chain),
            ["getPrice(address,uint40)((uint256,uint256))", token_address, 2 * 86400],  # 2 days
            [(name, _min_of_low_and_high_price)],
        )

    def getOraclePrice(name: str, token_address: str) -> Call:
        return Call(
            ROOT_PRICE_ORACLE(chain),
            ["getPriceInEth(address)(uint256)", token_address],
            [(name, safe_normalize_with_bool_success)],
        )

    blocks_to_get_incentive_token_prices = swapped_df["block"].unique()

    incentive_stats_calls = [getIncentiveTokenPrice(symbol, addr) for addr, symbol in token_address_to_symbol.items()]
    incentive_stats_token_prices_df = get_raw_state_by_blocks(
        incentive_stats_calls, blocks_to_get_incentive_token_prices, chain
    )

    oracle_price_calls = [getOraclePrice(symbol, addr) for addr, symbol in token_address_to_symbol.items()]
    oracle_price_df = get_raw_state_by_blocks(oracle_price_calls, blocks_to_get_incentive_token_prices, chain)

    achieved_eth_price_df = pd.pivot(
        swapped_df, index="date", columns="sellTokenAddress", values="achieved_token_price_in_eth"
    )

    achieved_eth_price_df.columns = [token_address_to_symbol[c] for c in achieved_eth_price_df.columns]

    return incentive_stats_token_prices_df, oracle_price_df, achieved_eth_price_df


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
