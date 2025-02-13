import pandas as pd
import plotly.express as px
from multicall import Call

from mainnet_launch.abis import LIQUIDATION_ROW_ABI
from mainnet_launch.constants import ROOT_PRICE_ORACLE, WETH, LIQUIDATION_ROW, AutopoolConstants


from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column

from mainnet_launch.data_fetching.get_events import fetch_events

from mainnet_launch.data_fetching.get_state_by_block import (
    build_blocks_to_use,
    get_raw_state_by_blocks,
    get_state_by_one_block,
    safe_normalize_with_bool_success,
)
from mainnet_launch.destinations import get_destination_details, DestinationDetails

import numpy as np


def _make_destination_tvl_calls(autopool: AutopoolConstants) -> list[Call]:
    calls = []
    for destination in get_destination_details(autopool):
        if destination.vaultAddress != autopool.autopool_eth_addr:
            floor_price_call = Call(
                ROOT_PRICE_ORACLE(destination.autopool.chain),
                [
                    "getFloorCeilingPrice(address,address,address,bool)(uint256)",
                    destination.dexPool,
                    destination.lpTokenAddress,
                    WETH(destination.autopool.chain),
                    False,
                ],
                [(f"{destination.vaultAddress}_floor", safe_normalize_with_bool_success)],
            )
            ceiling_price_call = Call(
                ROOT_PRICE_ORACLE(destination.autopool.chain),
                [
                    "getFloorCeilingPrice(address,address,address,bool)(uint256)",
                    destination.dexPool,
                    destination.lpTokenAddress,
                    WETH(destination.autopool.chain),
                    True,
                ],
                [(f"{destination.vaultAddress}_ceiling", safe_normalize_with_bool_success)],
            )
            destination_total_supply_call = Call(
                destination.vaultAddress,
                ["totalSupply()(uint256)"],
                [(f"{destination.vaultAddress}_totalSupply", safe_normalize_with_bool_success)],
            )

            calls.extend([floor_price_call, ceiling_price_call, destination_total_supply_call])

    return calls


def _get_daily_destination_incentive_apr_and_tvl(autopool: AutopoolConstants) -> pd.DataFrame:
    calls = _make_destination_tvl_calls(autopool)
    blocks = build_blocks_to_use(autopool.chain, approx_num_blocks_per_day=1)

    df = get_raw_state_by_blocks(calls, blocks, autopool.chain, include_block_number=True)
    for destination in get_destination_details(autopool):
        if destination.vaultAddress != autopool.autopool_eth_addr:
            df[f"{destination.vaultAddress}_TVL"] = df[f"{destination.vaultAddress}_totalSupply"] * (
                (df[f"{destination.vaultAddress}_floor"] + df[f"{destination.vaultAddress}_ceiling"]) / 2
            )

    daily_df = df.resample("1D").last()
    incentiveApr_df = fetch_destination_summary_stats(autopool, "incentiveApr").resample("1d").last()

    for destination in get_destination_details(autopool):
        if destination.vaultAddress != autopool.autopool_eth_addr:
            daily_df[f"{destination.vaultAddress}_incentive_apr"] = incentiveApr_df[destination.vault_name]

    return daily_df




def _fetch_all_vault_liquidated_events_df(autopool: AutopoolConstants) -> pd.DataFrame:

    lr_contract = autopool.chain.client.eth.contract(LIQUIDATION_ROW(autopool.chain), abi=LIQUIDATION_ROW_ABI)
    VaultLiquidated_df = add_timestamp_to_df_with_block_column(
        fetch_events(
            lr_contract.events.VaultLiquidated,
        ),
        autopool.chain,
    )
    VaultLiquidated_df["weth"] = (
        VaultLiquidated_df["amount"] / 1e18
    )  # weth received for selling rewardstokens associated with vault

    return VaultLiquidated_df


def make_daily_data_df(autopool:AutopoolConstants) -> pd.DataFrame:
    daily_df = _get_daily_destination_incentive_apr_and_tvl(autopool)
    VaultLiquidated_df = _fetch_all_vault_liquidated_events_df(autopool)
    for destination in get_destination_details(autopool):
        if destination.vaultAddress != autopool.autopool_eth_addr:

            daily_incentive_token_sales = VaultLiquidated_df[VaultLiquidated_df['vault'].str.lower() == destination.vaultAddress.lower()].resample('1d')[['weth']].sum()
            daily_df[f'{destination.vaultAddress}_daily_incentives_weth'] = daily_incentive_token_sales

    return daily_df



if __name__ == "__main__":
    from mainnet_launch.pages.solver_diagnostics.solver_diagnostics import _load_solver_df, AUTO_ETH
