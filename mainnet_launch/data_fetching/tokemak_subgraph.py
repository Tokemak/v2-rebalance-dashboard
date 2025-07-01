"""Helper methods to fetch data from the tokemak subgraph"""

from pprint import pprint

import requests
import pandas as pd
from mainnet_launch.constants import AutopoolConstants, SONIC_USD
from web3 import Web3

# TODO this fetches everything from 0, duplicates fetching
# is using subgraph so not really a big issue since the subgraph is very fast and this is run once/day

# TODO Fix TokenValues Root Price Oracle

# conflict with autoETH oracle?
# fix rebalance event queries
# ask nick if we are keeping the new or old schema
# https://subgraph.satsuma-prod.com/tokemak/v2-gen3-sonic-mainnet/playground


class TokemakSubgraphError(Exception):
    pass


def run_query_with_paginate(api_url: str, query: str, variables: dict, data_col: str) -> pd.DataFrame:
    """
    Helper to page through a GraphQL connection using `first`/`skip`.
    """

    all_records = []
    skip = 0

    if ("$first" not in query) or ("$skip" not in query):
        raise ValueError("")

    while True:
        vars_with_pagination = {**variables, "first": 500, "skip": skip}
        resp = requests.post(api_url, json={"query": query, "variables": vars_with_pagination})
        resp.raise_for_status()

        response_json = resp.json()
        if "errors" in response_json:
            raise TokemakSubgraphError("Query must contain `first` and `skip` variables" + "\n" + query)
        batch = response_json["data"][data_col]

        if not batch:
            break

        all_records.extend(batch)
        skip += 500

    df = pd.DataFrame.from_records(all_records)
    return df


def _fetch_autopool_rebalance_events_from_subgraph(autopool: AutopoolConstants) -> pd.DataFrame:
    if autopool == SONIC_USD:
        api_url = f"https://subgraph.satsuma-prod.com/108d48ba91e3/tokemak/v2-gen3-{autopool.chain.name}-mainnet2/api"
    else:
        api_url = f"https://subgraph.satsuma-prod.com/108d48ba91e3/tokemak/v2-gen3-{autopool.chain.name}-mainnet/api"

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

    df = run_query_with_paginate(
        api_url,
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

    return df


def _fetch_tx_hash_to_swap_cost_offset(autopool: AutopoolConstants) -> dict[str, int]:

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

    df = run_query_with_paginate(
        f"https://subgraph.satsuma-prod.com/108d48ba91e3/tokemak/v2-gen3-{autopool.chain.name}-mainnet/api",
        metrics_query,
        variables={},
        data_col="rebalanceBetweenDestinations",
    )

    if not df.empty:
        df["swapOffsetPeriod"] = df["swapOffsetPeriod"].astype(int)
    else:
        df = pd.DataFrame(columns=["transactionHash", "swapOffsetPeriod"])

    tx_hash_to_swap_cost_offset = df.set_index("transactionHash")["swapOffsetPeriod"].to_dict()
    return tx_hash_to_swap_cost_offset


def fetch_autopool_rebalance_events_from_subgraph(autopool: AutopoolConstants) -> pd.DataFrame:

    df = _fetch_autopool_rebalance_events_from_subgraph(autopool)
    df = df.sort_values("blockNumber")

    df["datetime_executed"] = pd.to_datetime(
        df["timestamp"].astype(int),
        unit="s",
        utc=True,
    )
    tx_hash_to_swap_cost_offset = _fetch_tx_hash_to_swap_cost_offset(autopool)
    df["swapOffsetPeriod"] = df["transactionHash"].map(lambda tx: tx_hash_to_swap_cost_offset.get(tx))
    return df


if __name__ == "__main__":
    from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants, USDC, WETH, AUTO_ETH, SONIC_USD

    # df = fetch_autopool_rebalance_events_from_subgraph(AUTO_ETH)

    # print(df.columns)

    df = fetch_autopool_rebalance_events_from_subgraph(SONIC_USD)
    pass
