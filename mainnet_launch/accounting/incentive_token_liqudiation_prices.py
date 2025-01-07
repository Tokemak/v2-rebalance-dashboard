import pandas as pd
import streamlit as st
from multicall import Call
import plotly.subplots as sp
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    get_state_by_one_block,
    identity_with_bool_success,
    safe_normalize_with_bool_success,
)
from mainnet_launch.data_fetching.new_databases import (
    write_dataframe_to_table,
    load_table,
    run_read_only_query,
    get_earliest_block_from_table_with_chain,
)
from mainnet_launch.data_fetching.should_update_database import should_update_table

from mainnet_launch.data_fetching.get_events import fetch_events

from mainnet_launch.constants import (
    INCENTIVE_PRICNIG_STATS,
    LIQUIDATION_ROW,
    ROOT_PRICE_ORACLE,
    ALL_CHAINS,
    ChainData,
)

from mainnet_launch.abis.abis import DESTINATION_DEBT_REPORTING_SWAPPED_ABI


INCENTIVE_TOKEN_PRICES_TABLE_NAME = "INCENTIVE_TOKEN_PRICES_AT_LIQUIDATION"


def _add_achieved_price_column(swapped_df: pd.DataFrame, token_address_to_decimals: dict):
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


def _fetch_reward_token_price_during_liquidation_from_external_source(
    chain: ChainData, start_block: int
) -> pd.DataFrame:
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

    swapped_df = _add_achieved_price_column(swapped_df, token_address_to_decimals)

    # not used in plots, but could be useful to see size later
    swapped_df["weth_received"] = swapped_df["buyTokenAmountReceived"] / 1e18

    long_swapped_df = swapped_df[["block", "sellTokenAddress", "achieved_price", "weth_received"]].copy()

    long_oracle_prices_df = pd.melt(
        oracle_price_df,
        id_vars=["block"],
        var_name="sellTokenAddress",
        value_name="oracle_price",
    )

    long_incentive_calculator_prices = pd.melt(
        incentive_calculator_price_df,
        id_vars=["block"],
        var_name="sellTokenAddress",
        value_name="incentive_calculator_price",
    )

    long_swapped_df = pd.merge(long_swapped_df, long_oracle_prices_df, on=["block", "sellTokenAddress"], how="left")
    long_swapped_df = pd.merge(
        long_swapped_df, long_incentive_calculator_prices, on=["block", "sellTokenAddress"], how="left"
    )

    timestamp_df = oracle_price_df.reset_index()[["timestamp", "block"]]

    long_swapped_df = pd.merge(long_swapped_df, timestamp_df, on=["block"], how="left")
    long_swapped_df["tokenSymbol"] = long_swapped_df["sellTokenAddress"].apply(lambda x: token_address_to_symbol[x])
    long_swapped_df["chain"] = chain.name

    return long_swapped_df


def add_new_reward_token_swapped_events_to_table():
    for chain in ALL_CHAINS:
        # TODO pick one name for these tables and dataframes and stick with it
        highest_block_already_fetched = get_earliest_block_from_table_with_chain(
            INCENTIVE_TOKEN_PRICES_TABLE_NAME, chain
        )
        new_swapped_events_df = _fetch_reward_token_price_during_liquidation_from_external_source(
            chain, highest_block_already_fetched
        )
        write_dataframe_to_table(new_swapped_events_df, INCENTIVE_TOKEN_PRICES_TABLE_NAME)


def make_histogram_subplots(df: pd.DataFrame, col: str, title: str):
    # this makes sure that the charts are in the same order
    sold_reward_tokens = sorted(list(df["tokenSymbol"].unique()))
    num_cols = 3
    num_rows = (len(sold_reward_tokens) // num_cols) + 1  # not certain on this math
    fig = sp.make_subplots(
        rows=num_rows,
        cols=num_cols,
        subplot_titles=sold_reward_tokens,
        x_title="Percent Difference Achieved vs Expected",
        y_title="Percent of Reward Liqudations",
    )

    bin_range = [df[col].min(), df[col].max()]
    bin_range = [int(bin_range[0]) - 1, int(bin_range[1]) + 1]
    bin_width = 1  # 1% width per bin

    for i, token in enumerate(sold_reward_tokens):
        token_series = df[df["tokenSymbol"] == token][col]
        this_row = (i // num_rows) + 1
        this_col = (i % num_cols) + 1

        hist = go.Histogram(
            histnorm="percent", x=token_series, xbins=dict(start=bin_range[0], end=bin_range[1], size=bin_width)
        )

        fig.add_trace(hist, row=this_row, col=this_col)
        fig.update_xaxes(range=bin_range, row=this_row, col=this_col, autorange=False)

    fig.update_layout(height=600, width=900, showlegend=False, title=title)
    return fig


def _get_only_some_incentive_tokens_prices(
    chain: ChainData,
    start_time: pd.Timestamp,
) -> pd.DataFrame:
    query = f"""
    SELECT *
    FROM {INCENTIVE_TOKEN_PRICES_TABLE_NAME}
    WHERE chain = ?
      AND timestamp > ?
      AND incentive_calculator_price != 0;
    """
    formatted_start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")

    swapped_df = run_read_only_query(query, params=(chain.name, formatted_start_time))

    swapped_df["incentive percent diff to achieved"] = (
        100
        * (swapped_df["incentive_calculator_price"] - swapped_df["achieved_price"])
        / swapped_df["incentive_calculator_price"]
    )
    swapped_df["oracle percent diff to achieved"] = (
        100 * (swapped_df["oracle_price"] - swapped_df["achieved_price"]) / swapped_df["oracle_price"]
    )
    return swapped_df


def fetch_and_render_reward_token_achieved_vs_incentive_token_price():

    if should_update_table(INCENTIVE_TOKEN_PRICES_TABLE_NAME):
        add_new_reward_token_swapped_events_to_table()

    today = datetime.now(timezone.utc)
    thirty_days_ago = today - timedelta(days=30)
    year_ago = today - timedelta(days=365)

    for start_time in [thirty_days_ago, year_ago]:
        for chain in ALL_CHAINS:
            st.subheader(f"Since {start_time.strftime('%Y-%m-%d')} {chain.name} Achieved vs Expected Token Price")
            chain_swapped_df = _get_only_some_incentive_tokens_prices(chain, start_time)
            st.plotly_chart(
                make_histogram_subplots(
                    chain_swapped_df,
                    col="incentive percent diff to achieved",
                    title=f"{chain.name} Pecent Difference Between Incentive Calculator and Achieved",
                ),
                use_container_width=True,
            )

            st.plotly_chart(
                make_histogram_subplots(
                    chain_swapped_df,
                    col="oracle percent diff to achieved",
                    title=f"{chain.name} Pecent Difference Between Oracle and Achieved",
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
            - Positive values indicate that we sold the incentive token for more than the Incentive Stats Price at that block. Eg there was price movement in our favor

            ## Achieved vs Oracle Price
            - This metric uses the Root Price Oracle to fetch the current price of the incentive token (typically via Chainlink)
            - Positive values indicate that we sold the incentive token for more than the Oracle Price at that block. 
            """
        )


if __name__ == "__main__":
    fetch_and_render_reward_token_achieved_vs_incentive_token_price()
