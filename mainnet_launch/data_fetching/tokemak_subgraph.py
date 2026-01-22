"""Helper methods to fetch data from the tokemak subgraph"""

import requests
import random
import time


import pandas as pd
from web3 import Web3


from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.database.postgres_operations import _exec_sql_and_cache
from mainnet_launch.database.schema.full import RebalanceEvents


class TokemakSubgraphError(Exception):
    pass


def run_query_with_paginate(
    api_url: str, query: str, variables: dict, data_col: str, batch_size: int = 500, first_n_records: int | None = None
) -> pd.DataFrame:
    """
    Helper to page through a GraphQL connection using `first`/`skip`.
    """
    all_records = []
    skip = 0

    if ("$first" not in query) or ("$skip" not in query):
        raise TokemakSubgraphError("Query must contain `first` and `skip` variables" + "\n" + query)

    while True:
        vars_with_pagination = {**variables, "first": batch_size, "skip": skip}
        time.sleep(5 + random.choice([_ for _ in range(5)]))
        # Origin, https/reblance-dashboard as an outgoing request header
        headers = {"Origin": "https://autopool-dashboard-data-fetching.com"}

        resp = requests.post(api_url, json={"query": query, "variables": vars_with_pagination, "headers": headers})
        resp.raise_for_status()

        response_json = resp.json()
        if "errors" in response_json:
            raise TokemakSubgraphError("Query must contain `first` and `skip` variables" + "\n" + query)
        batch = response_json["data"][data_col]

        if not batch:
            break

        all_records.extend(batch)
        skip += batch_size
        if first_n_records is not None and len(all_records) >= first_n_records:
            break

    df = pd.DataFrame.from_records(all_records)
    return df


def _get_highest_block_of_rebalance_events_already_saved_in_database(autopool: AutopoolConstants) -> int:
    query = f"""
        SELECT MAX(transactions.block) AS block
        FROM rebalance_events
        JOIN transactions 
        ON rebalance_events.tx_hash = transactions.tx_hash
        WHERE rebalance_events.autopool_vault_address = '{autopool.autopool_eth_addr}'
    """

    df = _exec_sql_and_cache(query)
    if not df.empty and df["block"].iloc[0] is not None:
        return int(df["block"].iloc[0])
    else:
        return autopool.block_deployed


def _fetch_raw_rebalance_events_from_subgraph(autopool: AutopoolConstants) -> pd.DataFrame:
    highest_block_already_seen = _get_highest_block_of_rebalance_events_already_saved_in_database(autopool)
    highest_block_already_seen = 0
    query = """
    query(
      $autoEthAddress: String!
      $first: Int!
      $skip: Int!
      $minBlock: BigInt!
    ) {
      autopoolRebalances(
        first: $first
        skip: $skip
        orderBy: id
        orderDirection: desc
        where: {
          autopool: $autoEthAddress
          blockNumber_gt: $minBlock
        }
      ) {
        transactionHash
        timestamp
        blockNumber
        tokenIn {
          id
          decimals
        }
        destinationInAddress
        tokenInAmount
        tokenOut {
          id
          decimals
        }
        destinationOutAddress
        tokenOutAmount
      }
    }
    """

    df = run_query_with_paginate(
        autopool.chain.tokemak_subgraph_url,
        query,
        variables={
            "autoEthAddress": autopool.autopool_eth_addr.lower(),
            "minBlock": highest_block_already_seen,
        },
        data_col="autopoolRebalances",
    )

    return df


def _postprocess_rebalance_events_df(autopool: AutopoolConstants, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df["autopool"] = autopool.autopool_eth_addr
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
    df["swapOffsetPeriod"] = None
    return df


def fetch_new_autopool_rebalance_events_from_subgraph(autopool: AutopoolConstants) -> pd.DataFrame:
    df = _fetch_raw_rebalance_events_from_subgraph(autopool)
    df = _postprocess_rebalance_events_df(autopool, df)
    return df


if __name__ == "__main__":
    
    from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants, USDC, WETH, AUTO_ETH, SONIC_USD, ARB_USD
    df = fetch_new_autopool_rebalance_events_from_subgraph(AUTO_ETH)
