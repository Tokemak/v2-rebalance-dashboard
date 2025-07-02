import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
import plotly.express as px
from multicall import Call
import streamlit as st


from mainnet_launch.data_fetching.tokemak_subgraph import run_query_with_paginate
from mainnet_launch.constants import *
from mainnet_launch.data_fetching.get_state_by_block import get_raw_state_by_blocks, safe_normalize_6_with_bool_success


autopool_address_to_name = {a.autopool_eth_addr: a.name for a in ALL_AUTOPOOLS}


def get_rebalance_volumne_raw_data(chain: ChainData):
    query = """
    query getAutopoolRebalances($first: Int!, $skip: Int!) {
      autopoolRebalances(
        first: $first,
        skip: $skip,
        orderBy: timestamp,
        orderDirection: asc,
      ) {
        autopool
        destinationInName
        destinationOutName
        timestamp
        blockNumber
        transactionHash
        tokenOutValueInEth
        tokenOutValueBaseAsset
      }
    }
    """

    if chain == ETH_CHAIN:
        api_url = os.environ["TOKEMAK_ETHEREUM_SUBGRAPH_URL"]
    elif chain == BASE_CHAIN:
        api_url = os.environ["TOKEMAK_BASE_SUBGRAPH_URL"]
    elif chain == SONIC_CHAIN:
        api_url = os.environ["TOKEMAK_SONIC_SUBGRAPH_URL"]

    variables = {}
    df = run_query_with_paginate(api_url, query, variables, "autopoolRebalances")
    df["chain_id"] = chain.chain_id
    df["autopool_name"] = df["autopool"].apply(
        lambda x: autopool_address_to_name[ETH_CHAIN.client.toChecksumAddress(x)]
    )
    df["datetime"] = pd.to_datetime(df["timestamp"].astype(int), unit="s", utc=True)
    df["date"] = df["datetime"].dt.date

    df["block"] = df["blockNumber"].astype(int)
    df["tokenOutValueInEth_norm"] = df["tokenOutValueInEth"].apply(lambda x: int(x) / 1e18)
    return df


def get_sonic_rebalance_volumne_df():

    df = get_rebalance_volumne_raw_data(SONIC_CHAIN)

    if np.any(df["autopool_name"] != "sonicUSD"):
        raise ValueError("Need custom logic to get the Sonic `S` price to USDC")

    df["computed_usd_volumne"] = df["tokenOutValueBaseAsset"].apply(lambda x: int(x) / 1e6)

    return df.set_index("datetime").sort_index()


def get_mainnet_rebalance_volumne_df():
    df = get_rebalance_volumne_raw_data(ETH_CHAIN)

    calls = [
        Call(
            ROOT_PRICE_ORACLE(ETH_CHAIN),
            ["getPriceInQuote(address,address)(uint256)", WETH(ETH_CHAIN), USDC(ETH_CHAIN)],
            [("mainnet_ETH_TO_USDC_price", safe_normalize_6_with_bool_success)],
        )
    ]

    blocks = df["blockNumber"].astype(int)
    base_weth_price_df = get_raw_state_by_blocks(calls, blocks, ETH_CHAIN, include_block_number=True)
    full_df = pd.merge(df, base_weth_price_df, on="block")

    full_df["computed_usd_volumne"] = full_df["tokenOutValueInEth_norm"] * full_df["mainnet_ETH_TO_USDC_price"]
    return full_df.set_index("datetime").sort_index()


def get_base_rebalance_volume_df():
    df = get_rebalance_volumne_raw_data(BASE_CHAIN)

    calls = [
        Call(
            ROOT_PRICE_ORACLE(BASE_CHAIN),
            ["getPriceInQuote(address,address)(uint256)", WETH(BASE_CHAIN), USDC(BASE_CHAIN)],
            [("base_chain_ETH_to_USDC_price", safe_normalize_6_with_bool_success)],
        )
    ]

    blocks = df["blockNumber"].astype(int)

    base_weth_price_df = get_raw_state_by_blocks(calls, blocks, BASE_CHAIN, include_block_number=True)
    full_df = pd.merge(df, base_weth_price_df, on="block")

    full_df["computed_usd_volumne"] = full_df["tokenOutValueInEth_norm"] * full_df["base_chain_ETH_to_USDC_price"]
    return full_df.set_index("datetime").sort_index()


@st.cache_data(ttl=60 * 60, show_spinner=False)  # save data for an hour
def fetch_all_time_cumulative_usd_volume() -> pd.DataFrame:
    dfs = []

    with ThreadPoolExecutor(max_workers=3) as executor:
        # submit returns Future objects
        futures = [
            executor.submit(get_base_rebalance_volume_df),
            executor.submit(get_mainnet_rebalance_volumne_df),
            executor.submit(get_sonic_rebalance_volumne_df),
        ]

        for future in as_completed(futures):
            dfs.append(future.result())

    raw_df = pd.concat(dfs, axis=0).reset_index()

    volume_by_pool = raw_df.groupby(["datetime", "autopool_name"])["computed_usd_volumne"].sum().reset_index()

    pivoted = (
        volume_by_pool.pivot(index="datetime", columns="autopool_name", values="computed_usd_volumne")
        .fillna(0)
        .resample("1d")
        .sum()
    )
    cumulaitive_volume_by_autopool = pivoted.sort_index().cumsum()
    return cumulaitive_volume_by_autopool, raw_df


def fetch_and_render_cumulative_volume():
    with st.spinner("Loading cumulative volume from rebalance events (takes ~30s)…"):
        cumulative_volumne_by_autopool, raw_df = fetch_all_time_cumulative_usd_volume()

    fig = px.bar(
        cumulative_volumne_by_autopool,
        title="Cumulative USD Volume by Autopool",
        labels={"value": "Cumulative USD Volume", "date": "Date", "autopool_name": "Autopool"},
    )
    st.plotly_chart(fig, use_container_width=True)

    latest_day = cumulative_volumne_by_autopool.tail(1).copy().round()
    latest_day["Total"] = latest_day.sum(axis=1)
    cols = ["Total"] + [col for col in latest_day.columns if col != "Total"]
    latest_day = latest_day[cols]
    st.dataframe(latest_day)

    latest_event_utc = raw_df.index.max()
    readable = latest_event_utc.strftime("%B %d, %Y at %I:%M %p %Z")

    st.write(f"USD volume is current as of: **{readable}**")
    csv_bytes = raw_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download raw data as CSV",
        data=csv_bytes,
        file_name="all_autopool_rebalance_events_with_usd_volume.csv",
        mime="text/csv",
    )

    with st.expander("Details"):
        st.markdown(
            """
        Looks at rebalance events from AutopoolRebalances.tokenOutValueInEth in the subgraph and the safe price in ETH from our oracle at that block.
        Not setup for autoS yet
          """
        )


if __name__ == "__main__":
    fetch_all_time_cumulative_usd_volume()
