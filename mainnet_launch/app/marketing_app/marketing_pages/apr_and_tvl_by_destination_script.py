"""See script for dan"""

import pandas as pd
from mainnet_launch.constants import *
from mainnet_launch.data_fetching.tokemak_subgraph import run_query_with_paginate

import requests
import os


def response_to_dataframe(autopool: AutopoolConstants, response_json):
    records = response_json.get("data", {}).get("autopoolDayDatas", [])

    df = pd.DataFrame(records)


    return df


def fetch_apr_data_from_subgraph(autopool: AutopoolConstants):
    query = """
    query GetAutopoolDayData($address: String!) {
        autopoolDayDatas(
        where: { id_contains_nocase: $address }
        orderBy: timestamp
        orderDirection: asc
        first: 1000
        ) {
        autopoolDay30MAApy
        autopoolDay7MAApy

        autopoolDay1MAApy
        autopoolApy 

        rewarderDay30MAApy
        rewarderDay7MAApy
        rewarderApy
        date
        }
    }
    """
    if autopool.chain == ETH_CHAIN:
        api_url = os.environ["TOKEMAK_ETHEREUM_SUBGRAPH_URL"]
    elif autopool.chain == BASE_CHAIN:
        api_url = os.environ["TOKEMAK_BASE_SUBGRAPH_URL"]
    elif autopool.chain == SONIC_CHAIN:
        api_url = os.environ["TOKEMAK_SONIC_SUBGRAPH_URL"]

    variables = {"address": autopool.autopool_eth_addr.lower()}

    # response = requests.post(api_url, json={"query": query, "variables": variables})
    # data = response.json()
    df = run_query_with_paginate(api_url, query, data)



    df["date"] = pd.to_datetime(df["date"], utc=True)
    df.set_index("date  ", inplace=True)

    for col in ["rewarderDay30MAApy" "rewarderDay7MAApy" "rewarderApy"]:
        df[col] = 100 * df[col].apply(lambda x: int(x) / 1e18 if x is not None else None)

    for col in ["autopoolDay30MAApy", "autopoolDay7MAApy", "autopoolDay1MAApy", "autopoolApy"]:
        df[col] = 100 * df[col].apply(
            lambda x: int(x) / 10 ** (autopool.base_asset_decimals) if x is not None else None
        )

    return df


# from mainnet_launch.pages.autopool_exposure.allocation_over_time import _fetch_tvl_by_asset_and_destination


def fetch_percent_allocation_at_the_end_of_each_day(autopool):
    safe_value_by_destination, safe_value_by_asset, backing_value_by_destination = _fetch_tvl_by_asset_and_destination(
        autopool
    )
    percent_tvl_by_destination = 100 * safe_value_by_destination.div(
        safe_value_by_destination.sum(axis=1).replace(0, None), axis=0
    )
    return percent_tvl_by_destination


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    for autopool in ALL_AUTOPOOLS:
        if autopool.chain.chain_id == 146:
            # as of June 25 sonic is broken, the API URL is wrong
            continue
        apr_df = fetch_apr_data_from_subgraph(autopool)
        percent_allocation_df = fetch_percent_allocation_at_the_end_of_each_day(autopool)
        df = pd.merge(percent_allocation_df, apr_df, left_index=True, right_index=True, how="left").round(2)
        filename = f"{autopool.name}_percent_allocation_at_the_end_of_each_day.csv"
        output_path = os.path.join(script_dir, filename)
        df.to_csv(output_path)
        print(df.tail())
        print(f"Wrote {df.shape=} for {autopool.name}")

    # apr_df = fetch_apr_data_from_subgraph(AUTO_ETH)
    # percent_allocation_df = fetch_percent_allocation_at_the_end_of_each_day(AUTO_ETH)
    # autoETH_df = pd.merge(percent_allocation_df, apr_df, left_index=True, right_index=True, how='left').round(2)
    # autoETH_df.to_csv('./autoETH_percent_allocation_at_the_end_of_each_day.csv')

    # apr_df = fetch_apr_data_from_subgraph(AUTO_USD)
    # percent_allocation_df = fetch_percent_allocation_at_the_end_of_each_day(AUTO_USD)
    # autoUSD_df = pd.merge(percent_allocation_df, apr_df, left_index=True, right_index=True, how='left').round(2)
    # autoUSD_df.to_csv('./autoUSD_percent_allocation_at_the_end_of_each_day.csv')


if __name__ == "__main__":
    from mainnet_launch.constants import AUTO_ETH

    df = fetch_apr_data_from_subgraph(AUTO_ETH)
