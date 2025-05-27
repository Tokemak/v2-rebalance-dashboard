"""Helper methods to fetch data from the tokemak subgraph"""

import requests
import pandas as pd
from mainnet_launch.constants import *
from mainnet_launch.constants import ChainData, AutopoolConstants
from web3 import Web3


def _get_subgraph_api(chain: ChainData):
    if chain == "eth":
        api_url = "https://subgraph.satsuma-prod.com/108d48ba91e3/tokemak/v2-gen3-eth-mainnet/api"
    elif chain == "base":
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
        batch = resp.json()["data"][data_col]

        if not batch:
            break

        all_records.extend(batch)
        skip += 500

    df = pd.DataFrame.from_records(all_records)
    return df


def fetch_autopool_rebalance_events_from_subgraph(autopool: AutopoolConstants, chain: ChainData) -> list[dict]:
    """
    fetch the
    """
    subgraph_url = _get_subgraph_api(chain)

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

        tokenInAmount
        tokenIn { id decimals }
        tokenOutAmount
        tokenOut { id decimals }

        destinationInAddress
        destinationOutAddress
      }
    }
    """

    df = run_query_with_paginate(
        subgraph_url,
        query,
        variables={"autoEthAddress": autopool.autopool_eth_addr.lower()},
        data_col="autopoolRebalances",
    )

    df["blockNumber"] = df["blockNumber"].astype(int)

    df["tokenInAddress"] = df["tokenIn"].apply(lambda x: Web3.toChecksumAddress(x["id"]))
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
    return df
