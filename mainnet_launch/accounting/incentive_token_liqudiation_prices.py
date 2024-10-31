import pandas as pd
import streamlit as st
from multicall import Call
import plotly.express as px
import json


from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    get_state_by_one_block,
    identity_with_bool_success,
)
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column

from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.constants import INCENTIVE_PRICNIG_STATS, LIQUIDATION_ROW, eth_client, CACHE_TIME


def fetch_and_render_reward_token_achieved_vs_incentive_token_price():
    achieved_eth_price_df, incentive_token_prices_df = fetch_reward_token_achieved_vs_incentive_token_price()

    percent_diff_fig = _make_histogram_of_percent_diff(achieved_eth_price_df, incentive_token_prices_df)

    liquidation_price_plots = _make_liquidation_vs_incentive_token_prices_fig(
        achieved_eth_price_df, incentive_token_prices_df
    )

    st.plotly_chart(percent_diff_fig, use_container_width=True)
    for fig in liquidation_price_plots:
        st.plotly_chart(fig, use_container_width=True)


@st.cache_data(ttl=CACHE_TIME)
def fetch_reward_token_achieved_vs_incentive_token_price() -> tuple[pd.DataFrame, pd.DataFrame]:
    swapped_df = _build_swapped_df()

    achieved_eth_price_df = pd.pivot(
        swapped_df, index="date", columns="sellTokenAddress", values="achieved_token_price_in_eth"
    )

    incentive_token_prices_df = _fetch_incentive_token_price_df(swapped_df)

    symbol_calls = [
        Call(addr, ["symbol()(string)"], [(addr, identity_with_bool_success)]) for addr in achieved_eth_price_df.columns
    ]

    token_address_to_symbol = get_state_by_one_block(symbol_calls, eth_client.eth.block_number)
    achieved_eth_price_df.columns = [token_address_to_symbol[c] for c in achieved_eth_price_df.columns]
    incentive_token_prices_df.columns = [token_address_to_symbol[c] for c in incentive_token_prices_df.columns]

    incentive_token_prices_df = incentive_token_prices_df[achieved_eth_price_df.columns]

    return achieved_eth_price_df, incentive_token_prices_df


def _build_swapped_df():

    # This ABI is ABI is not on etherscan, you have to get it from the v2-core repo foundry contracts when it makes the ABIs

    # fmt: off
    SWAPPED_EVENT_ABI = json.loads(
        """[{"inputs":[{"internalType":"address","name":"aggregator","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[{"internalType":"uint256","name":"balanceNeeded","type":"uint256"},{"internalType":"uint256","name":"balanceAvailable","type":"uint256"}],"type":"error","name":"InsufficientBalance"},{"inputs":[],"type":"error","name":"InsufficientBuyAmount"},{"inputs":[{"internalType":"uint256","name":"buyTokenAmountReceived","type":"uint256"},{"internalType":"uint256","name":"buyAmount","type":"uint256"}],"type":"error","name":"InsufficientBuyAmountReceived"},{"inputs":[],"type":"error","name":"InsufficientSellAmount"},{"inputs":[],"type":"error","name":"SwapFailed"},{"inputs":[],"type":"error","name":"TokenAddressZero"},{"inputs":[{"internalType":"address","name":"sellTokenAddress","type":"address","indexed":true},{"internalType":"address","name":"buyTokenAddress","type":"address","indexed":true},{"internalType":"uint256","name":"sellAmount","type":"uint256","indexed":false},{"internalType":"uint256","name":"buyAmount","type":"uint256","indexed":false},{"internalType":"uint256","name":"buyTokenAmountReceived","type":"uint256","indexed":false}],"type":"event","name":"Swapped","anonymous":false},{"inputs":[],"stateMutability":"view","type":"function","name":"AGGREGATOR","outputs":[{"internalType":"address","name":"","type":"address"}]},{"inputs":[{"internalType":"struct SwapParams","name":"swapParams","type":"tuple","components":[{"internalType":"address","name":"sellTokenAddress","type":"address"},{"internalType":"uint256","name":"sellAmount","type":"uint256"},{"internalType":"address","name":"buyTokenAddress","type":"address"},{"internalType":"uint256","name":"buyAmount","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"},{"internalType":"bytes","name":"extraData","type":"bytes"},{"internalType":"uint256","name":"deadline","type":"uint256"}]}],"stateMutability":"nonpayable","type":"function","name":"swap","outputs":[{"internalType":"uint256","name":"buyTokenAmountReceived","type":"uint256"}]}]"""
    )
    # fmt: on
    contract = eth_client.eth.contract(LIQUIDATION_ROW, abi=SWAPPED_EVENT_ABI)
    swapped_df = add_timestamp_to_df_with_block_column(fetch_events(contract.events.Swapped))

    def _get_achieved_price(row):
        if (
            row["sellTokenAddress"] == "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        ):  # usdc #TODO swich to decimals check
            normalized_sell_amount = row["sellAmount"] / 1e6
        else:
            normalized_sell_amount = row["sellAmount"] / 1e18

        normalized_buy_amount = row["buyTokenAmountReceived"] / 1e18

        return normalized_buy_amount / normalized_sell_amount

    swapped_df["achieved_token_price_in_eth"] = swapped_df.apply(lambda row: _get_achieved_price(row), axis=1)
    swapped_df["date"] = swapped_df.index
    return swapped_df


def _fetch_incentive_token_price_df(swapped_df: pd.DataFrame):

    def _min_of_low_and_high_price(success, data):
        if success:
            fast, slow = data
            return min(fast, slow) / 1e18
        return None

    def getIncentiveTokenPrice(name: str, token_address: str) -> Call:
        return Call(
            INCENTIVE_PRICNIG_STATS,
            ["getPrice(address,uint40)((uint256,uint256))", token_address, 2 * 86400],  # 2 days
            [(name, _min_of_low_and_high_price)],
        )

    token_addresses = swapped_df["sellTokenAddress"].unique()
    calls = [getIncentiveTokenPrice(addr, addr) for addr in token_addresses]

    blocks_to_get_incentive_token_prices = swapped_df["block"].unique()
    incentive_token_prices_df = get_raw_state_by_blocks(calls, blocks_to_get_incentive_token_prices)
    return incentive_token_prices_df


def _make_liquidation_vs_incentive_token_prices_fig(achieved_eth_price_df, incentive_token_prices_df):
    figs = []
    for col in achieved_eth_price_df.columns:
        comparison_prices = pd.DataFrame(index=incentive_token_prices_df.index)
        comparison_prices[f"{col} achieved"] = achieved_eth_price_df[col]
        comparison_prices[f"{col} expected_price"] = incentive_token_prices_df[col]
        fig = px.scatter(comparison_prices, title=f"{col} achieved price vs expected price")
        figs.append(fig)
    return figs


def _make_histogram_of_percent_diff(incentive_token_prices_df, achieved_eth_price_df):
    percent_achieved_less_than_pricer_price = 100 * (
        (incentive_token_prices_df - achieved_eth_price_df) / incentive_token_prices_df
    )
    fig = px.histogram(percent_achieved_less_than_pricer_price, histnorm="percent")
    fig.update_xaxes(title_text="Percent diff between achieved vs min(fast, slow price)")

    return fig
