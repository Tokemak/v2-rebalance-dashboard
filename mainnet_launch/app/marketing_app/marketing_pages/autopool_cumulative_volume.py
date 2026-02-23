from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
import plotly.express as px
from multicall import Call
import streamlit as st
from web3 import Web3


from mainnet_launch.data_fetching.tokemak_subgraph import run_query_with_paginate
from mainnet_launch.constants import *
from mainnet_launch.data_fetching.get_state_by_block import get_raw_state_by_blocks, safe_normalize_6_with_bool_success


# TODO consider putting USD prices in to the database, so that it doesn't refetch them each time


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

    df = run_query_with_paginate(chain.tokemak_subgraph_url, query, {}, "autopoolRebalances")
    found_autopools = set(df["autopool"].apply(lambda x: Web3.toChecksumAddress(x)))
    valid_autopools = set(autopool_address_to_name.keys())
    if not found_autopools.issubset(valid_autopools):
        not_counted_autopools = found_autopools - valid_autopools
        st.write("Autopools not included in the data: ")
        for autopool in not_counted_autopools:
            st.write(f"Autopool: {autopool}, Chain ID: {chain.chain_id}")

        # reset to exclude autopools not in the valid set
        df = df[df["autopool"].apply(lambda x: Web3.toChecksumAddress(x) in valid_autopools)].reset_index(drop=True)

    df["chain_id"] = chain.chain_id
    df["autopool_name"] = df["autopool"].apply(lambda x: autopool_address_to_name[Web3.toChecksumAddress(x)])
    df["datetime"] = pd.to_datetime(df["timestamp"].astype(int), unit="s", utc=True)
    df["date"] = df["datetime"].dt.date
    df["block"] = df["blockNumber"].astype(int)
    df["tokenOutValueInEth_norm"] = df["tokenOutValueInEth"].apply(lambda x: int(x) / 1e18)
    return df


def get_sonic_rebalance_volumne_df():

    df = get_rebalance_volumne_raw_data(SONIC_CHAIN)

    if np.any(df["autopool_name"] != "sonicUSD"):
        raise ValueError("Need custom logic to get the Sonic `S` price to USDC")

    df["computed_usd_volume"] = df["tokenOutValueBaseAsset"].apply(lambda x: int(x) / 1e6)

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

    full_df["computed_usd_volume"] = full_df["tokenOutValueInEth_norm"] * full_df["mainnet_ETH_TO_USDC_price"]
    return full_df.set_index("datetime").sort_index()


def get_base_rebalance_volume_df():
    df = get_rebalance_volumne_raw_data(BASE_CHAIN)

    # TODO need to includ ERC here as well
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

    full_df["computed_usd_volume"] = full_df["tokenOutValueInEth_norm"] * full_df["base_chain_ETH_to_USDC_price"]
    return full_df.set_index("datetime").sort_index()


@st.cache_data(ttl=60 * 60, show_spinner=False)
def fetch_all_time_cumulative_usd_volume() -> pd.DataFrame:
    dfs = []

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(get_base_rebalance_volume_df),
            executor.submit(get_mainnet_rebalance_volumne_df),
            executor.submit(get_sonic_rebalance_volumne_df),
        ]

        for future in as_completed(futures):
            dfs.append(future.result())

    raw_df = pd.concat(dfs, axis=0).reset_index()

    raw_df.drop(columns=["base_chain_ETH_to_USDC_price", "mainnet_ETH_TO_USDC_price"], inplace=True)

    volume_by_pool = raw_df.groupby(["datetime", "autopool_name"])["computed_usd_volume"].sum().reset_index()
    cumulaitive_volume_by_autopool = (
        (
            volume_by_pool.pivot(index="datetime", columns="autopool_name", values="computed_usd_volume")
            .fillna(0)
            .resample("1d")
            .sum()
        )
        .sort_index()
        .cumsum()
    )

    rebalance_count_by_autopool = volume_by_pool.groupby("autopool_name").size().reset_index(name="rebalance_count")

    cumulaitive_volume_by_autopool["rebalance_count"] = cumulaitive_volume_by_autopool.index.map(
        rebalance_count_by_autopool.set_index("autopool_name")["rebalance_count"]
    )
    return cumulaitive_volume_by_autopool, raw_df


def fetch_and_render_cumulative_volume() -> None:
    with st.spinner("Loading cumulative volume from rebalance events (~30 seconds)"):
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
    latest_day = latest_day[cols].T

    latest_day.columns = ["Cumulative USD Volume"]
    st.dataframe(latest_day)

    latest_event_utc = raw_df["datetime"].max().strftime("%B %d, %Y at %I:%M %p %Z")

    st.write(f"USD volume is current as of: **{latest_event_utc}**")

    st.download_button(
        label="Download raw data as CSV",
        data=raw_df.to_csv(index=False).encode("utf-8"),
        file_name="all_autopool_rebalance_events_with_usd_volume.csv",
        mime="text/csv",
    )

    st.metric(label="Count of All Time Rebalance Events", value=f"{int(raw_df.shape[0])}")

    with st.expander("Details"):
        st.markdown(
            """
            - Rebalance events and size fetched from AutopoolRebalances.tokenOutValueInEth in the subgraph.
            - The safe price from WETH -> USDC from our RootPriceOracle on that chain at that block.
            - Data is cached for 1 hour.
            """
        )


if __name__ == "__main__":
    fetch_all_time_cumulative_usd_volume()
