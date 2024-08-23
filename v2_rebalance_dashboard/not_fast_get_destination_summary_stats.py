import pandas as pd

from multicall import Call
from v2_rebalance_dashboard.get_state_by_block import (
    sync_safe_get_raw_state_by_block,
    sync_get_raw_state_by_block_one_block,
    build_blocks_to_use,
    safe_normalize_with_bool_success,
    identity_with_bool_success,
    eth_client,
    identity_function,
)
from concurrent.futures import ThreadPoolExecutor
import concurrent

import plotly.express as px
import json

from v2_rebalance_dashboard.get_state_by_block import (
    sync_safe_get_raw_state_by_block,
    sync_get_raw_state_by_block_one_block,
    build_blocks_to_use,
    safe_normalize_with_bool_success,
)
import numpy as np


with open("/home/parker/Documents/Tokemak/v2-rebalance-dashboard/v2_rebalance_dashboard/vault_abi.json", "r") as fin:
    vault_abi = json.load(fin)

with open("/home/parker/Documents/Tokemak/v2-rebalance-dashboard/v2_rebalance_dashboard/strategy_abi.json", "r") as fin:
    strategy_abi = json.load(fin)

balETH_auto_pool_vault = "0x72cf6d7C85FfD73F18a83989E7BA8C1c30211b73"
vault_contract = eth_client.eth.contract(balETH_auto_pool_vault, abi=vault_abi)
BALANCER_AUTO_POOL = "0xB86723da7d02C91b5E421Ed7883C35f732556F13"  # AUTOPOOL ETH STRATEGY

autoPool = eth_client.eth.contract(BALANCER_AUTO_POOL, abi=strategy_abi)


def get_summary_stats_without_weights(vault, block, size: int, prefix: str) -> dict:
    clean_summary_stats = dict()
    for direction_name, direction_enum in zip(["in", "out"], [0, 1]):
        summary_stats = autoPool.functions.getDestinationSummaryStats(vault, direction_enum, size).call(
            block_identifier=block
        )

        base_apr = summary_stats[1] / 1e18
        fee_apr = summary_stats[2] / 1e18
        incentive_apr = (summary_stats[3] / 1e18) / 0.9  # hard code
        safe_total_supply = summary_stats[4] / 1e18
        price_return = summary_stats[5] / 1e18

        if direction_enum == 1:  # == 1 means APR out
            if price_return > 0:
                price_return = price_return / 0.75
        else:
            # price return weight going into a destination is 0 show this by making it None.
            price_return = None

        composite_return = summary_stats[9] / 1e18

        clean_summary_stats.update(
            {
                f"{prefix}_{direction_name}_base_apr": base_apr,
                f"{prefix}_{direction_name}_fee_apr": fee_apr,
                f"{prefix}_{direction_name}_incentive_apr": incentive_apr,
                f"{prefix}_{direction_name}_price_return": price_return,
                f"{prefix}_{direction_name}_composite_return": composite_return,
                f"{prefix}_{direction_name}_safe_total_supply": safe_total_supply,
            }
        )
    return clean_summary_stats


def makeDestinationRecordWithoutWeights(block, timestamp, vault, name):
    try:
        clean_summary_stats = get_summary_stats_without_weights(vault, block, size=0, prefix="NoWeights")
        record = {
            "vault": vault,
            "name": name,
            "block": block,
            "block_timestamp": timestamp,
            **clean_summary_stats,
        }
    except Exception as e:
        record = {
            "vault": vault,
            "name": name,
            "block": block,
            "block_timestamp": timestamp,
            "getDestinationSummaryStats_error": str(type(e)) + str(e),
        }

    return record


def get_block_timestamp(block):
    return eth_client.eth.get_block(block).timestamp


def _make_blocks_df(blocks: list[int]):
    with ThreadPoolExecutor() as executor:
        # Execute the get_block_timestamp function concurrently for each block
        timestamps = list(executor.map(get_block_timestamp, blocks))

    # Create a DataFrame with the retrieved timestamps
    df = pd.DataFrame(index=timestamps)
    df.index = pd.to_datetime(df.index, unit="s")
    df.index.name = "timestamp"
    df["block"] = blocks
    return df


def build_destination_df(blocks, vaults):
    block_df = _make_blocks_df(blocks)
    records = []
    with ThreadPoolExecutor(max_workers=25) as executor:
        future_to_record = {
            executor.submit(makeDestinationRecordWithoutWeights, block, timestamp, vault, name): (
                block,
                timestamp,
                vault,
                name,
            )
            for block, timestamp in zip(block_df["block"], block_df.index)
            for vault, name in zip(vaults["vaultAddress"], vaults["name"])
        }
        for future in concurrent.futures.as_completed(future_to_record):
            record = future.result()
            records.append(record)
    destination_df = pd.DataFrame.from_records(records)
    destination_df.set_index("block_timestamp", inplace=True)
    destination_df.sort_index(ascending=False, inplace=True)

    return destination_df


def make_destination_composite_apr_plots():

    blocks = build_blocks_to_use()
    vaults_df = pd.read_csv("/home/parker/Documents/Tokemak/v2-rebalance-dashboard/v2_rebalance_dashboard/vaults.csv")
    destination_df = build_destination_df(blocks, vaults_df)
    destination_df.to_csv("/home/parker/Documents/Tokemak/v2-rebalance-dashboard/destination_df.csv")

    destination_df = pd.read_csv("/home/parker/Documents/Tokemak/v2-rebalance-dashboard/destination_df.csv")
    # composite_apr_in_df = (100 * destination_df.pivot_table(
    #     values="NoWeights_in_composite_return", index="block_timestamp", columns="name")).clip(upper=100).replace(100, np.nan)
    composite_apr_out_df = (
        (
            100
            * destination_df.pivot_table(
                values="NoWeights_out_composite_return", index="block_timestamp", columns="name"
            )
        )
        .clip(upper=100)
        .replace(100, np.nan)
    )

    names = [
        "Tokemak-Wrapped Ether-Balancer ETHx/wstETH",
        #    'Tokemak-Wrapped Ether-Balancer ezETH-WETH Stable Pool',
        "Tokemak-Wrapped Ether-Balancer osETH/wETH StablePool",
        "Tokemak-Wrapped Ether-Balancer rETH Stable Pool",
        "Tokemak-Wrapped Ether-Balancer rsETH / ETHx",
        "Tokemak-Wrapped Ether-Balancer rsETH-WETH Stable Pool",
        "Tokemak-Wrapped Ether-Balancer stETH Stable Pool",
        "Tokemak-Wrapped Ether-Balancer swETH-WETH Stable Pool",
        "Tokemak-Wrapped Ether-Balancer weETH-WETH Stable Pool",
        #    'Tokemak-Wrapped Ether-Balancer weETH/ezETH/rswETH',
        "Tokemak-Wrapped Ether-Balancer weETH/rETH StablePool",
        "Tokemak-Wrapped Ether-Balancer wstETH-WETH Stable Pool",
        "Tokemak-Wrapped Ether-Gyroscope ECLP wstETH/cbETH",
        "Tokemak-Wrapped Ether-Gyroscope ECLP wstETH/wETH",
    ]

    fig = px.line(composite_apr_out_df[names])
    fig.update_layout(
        # not attached to these settings
        title="CRM out balETH destinations",
        xaxis_title="Date",
        yaxis_title="Composite Return out",
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=500,
        width=800,
    )
    return fig
