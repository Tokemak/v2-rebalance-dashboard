"""Helper methods to fetch data from the tokemak subgraph"""

import requests
import pandas as pd
from mainnet_launch.constants import ChainData, AutopoolConstants, ETH_CHAIN, BASE_CHAIN
from web3 import Web3


def _get_subgraph_api(chain: ChainData):
    if chain == ETH_CHAIN:
        api_url = "https://subgraph.satsuma-prod.com/108d48ba91e3/tokemak/v2-gen3-eth-mainnet/api"
    elif chain == BASE_CHAIN:
        api_url = "https://subgraph.satsuma-prod.com/108d48ba91e3/tokemak/v2-gen3-base-mainnet/api"
    else:
        raise ValueError("bad chain", chain)

    return api_url


def run_query_with_paginate(api_url: str, query: str, variables: dict, data_col: str) -> pd.DataFrame:
    """
    Helper to page through a GraphQL connection using `first`/`skip`.
    """

    all_records = []
    skip = 0

    while True:
        vars_with_pagination = {**variables, "first": 500, "skip": skip}
        resp = requests.post(api_url, json={"query": query, "variables": vars_with_pagination})
        resp.raise_for_status()

        response_json = resp.json()
        batch = response_json["data"][data_col]

        if not batch:
            break

        all_records.extend(batch)
        skip += 500

    df = pd.DataFrame.from_records(all_records)
    return df


def fetch_autopool_rebalance_events_from_subgraph(autopool: AutopoolConstants) -> list[dict]:
    subgraph_url = _get_subgraph_api(autopool.chain)

    query = """
    query($autoEthAddress: String!, $first: Int!, $skip: Int!) {
      autopoolRebalances(
        first: $first,
        skip: $skip,
        orderBy: id,
        orderDirection: desc,
        where: { autopool: $autoEthAddress }
      ) {
        transactionHash
        timestamp
        blockNumber
        autopool
        
        tokenIn{id decimals}
        destinationInAddress
        tokenInAmount
        
        tokenOut{id decimals }
        destinationOutAddress
        tokenOutAmount
      }
    }
    """
    # is safe tokenOutValueBaseAsset

    df = run_query_with_paginate(
        subgraph_url,
        query,
        variables={"autoEthAddress": autopool.autopool_eth_addr.lower()},
        data_col="autopoolRebalances",
    )

    df["blockNumber"] = df["blockNumber"].astype(int)

    df["tokenInAddress"] = df["tokenIn"].apply(
        lambda x: Web3.toChecksumAddress(x["id"])
    )  # these are the lp token addresses
    df["tokenOutAddress"] = df["tokenOut"].apply(lambda x: Web3.toChecksumAddress(x["id"]))

    df["destinationInAddress"] = df["destinationInAddress"].apply(lambda x: Web3.toChecksumAddress(x))
    df["destinationOutAddress"] = df["destinationOutAddress"].apply(lambda x: Web3.toChecksumAddress(x))

    df["tokenOutAmount"] = df.apply(
        lambda row: int(row["tokenOutAmount"]) / (10 ** int(row["tokenOut"]["decimals"])), axis=1
    )
    df["tokenInAmount"] = df.apply(
        lambda row: int(row["tokenInAmount"]) / (10 ** int(row["tokenIn"]["decimals"])), axis=1
    )

    df = df.sort_values("blockNumber")

    df["datetime_executed"] = pd.to_datetime(
        df["timestamp"].astype(int),
        unit="s",
        utc=True,
    )

    # 2) Fetch metrics and merge on transactionHash
    metrics_query = """
    query($first: Int!, $skip: Int!) {
      rebalanceBetweenDestinations(
        first: $first,
        skip: $skip
      ) {
        transactionHash
        swapOffsetPeriod
      }
    }
    """
    #        # predictedAnnualizedGain

    metrics_df = run_query_with_paginate(
        subgraph_url,
        metrics_query,
        variables={},
        data_col="rebalanceBetweenDestinations",
    )

    # Cast types if any metrics returned
    if not metrics_df.empty:
        metrics_df["swapOffsetPeriod"] = metrics_df["swapOffsetPeriod"].astype(int)
    else:
        metrics_df = pd.DataFrame(columns=["transactionHash", "swapOffsetPeriod"])

    # Left join, fill missing with None
    df = df.merge(metrics_df, on="transactionHash", how="left")
    df["swapOffsetPeriod"] = df["swapOffsetPeriod"].where(df["swapOffsetPeriod"].notna(), None)
    # df["predictedAnnualizedGain"] = df["predictedAnnualizedGain"].where(df["predictedAnnualizedGain"].notna(), None)

    return df


if __name__ == "__main__":
    from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants, USDC, WETH, AUTO_ETH

    fetch_autopool_rebalance_events_from_subgraph(AUTO_ETH)
