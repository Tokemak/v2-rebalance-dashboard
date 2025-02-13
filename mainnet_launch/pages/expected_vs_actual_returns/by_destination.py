import pandas as pd
import plotly.express as px
from multicall import Call

from mainnet_launch.abis import LIQUIDATION_ROW_ABI
from mainnet_launch.constants import ROOT_PRICE_ORACLE, WETH, LIQUIDATION_ROW, AutopoolConstants


from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import (
    _fetch_destination_summary_stats_from_external_source,
)
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column

from mainnet_launch.data_fetching.get_events import fetch_events

from mainnet_launch.data_fetching.get_state_by_block import (
    build_blocks_to_use,
    get_raw_state_by_blocks,
    get_state_by_one_block,
    safe_normalize_with_bool_success,
    identity_with_bool_success,
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


def _make_destination_last_virtual_price_calls(autopool: AutopoolConstants) -> list[Call]:
    # call destinationVaultAddress.stats() -> then destination underlyingStats() then  last lastVirtualPrice()

    destinations = get_destination_details(autopool)
    get_stats_calls = [
        Call(
            dest.vaultAddress,
            ["getStats()(address)"],
            [(dest.vaultAddress, identity_with_bool_success)],
        )
        for dest in destinations
    ]

    dest_to_stats = get_state_by_one_block(get_stats_calls, autopool.chain.client.eth.block_number, autopool.chain)
    # dest:stats dict

    underlyingStats_calls = [
        Call(
            stats,
            ["underlyerStats()(address)"],
            [(dest_vault_address, identity_with_bool_success)],
        )
        for dest_vault_address, stats in dest_to_stats.items()
        if stats is not None
    ]

    dest_to_underlying_stats = get_state_by_one_block(
        underlyingStats_calls, autopool.chain.client.eth.block_number, autopool.chain
    )

    last_virtual_price_calls = [
        Call(
            underlying_stats,
            ["lastVirtualPrice()(uint256)"],
            [(dest_vault_address, safe_normalize_with_bool_success)],
        )
        for dest_vault_address, underlying_stats in dest_to_underlying_stats.items()
        if underlying_stats is not None
    ]
    return last_virtual_price_calls


def _fetch_destination_last_virtual_price_df(autopool: AutopoolConstants, blocks: list[int]):
    last_virtual_price_calls = _make_destination_last_virtual_price_calls(autopool)
    last_snapshot_virtual_price_df = get_raw_state_by_blocks(
        last_virtual_price_calls, blocks, autopool.chain, include_block_number=True
    )
    return last_snapshot_virtual_price_df


def _fetch_in_and_out_summary_stats(autopool: AutopoolConstants, blocks: list[int]):
    out_summary_stats_df = _fetch_destination_summary_stats_from_external_source(autopool, blocks, direction="out")
    in_summary_stats_df = _fetch_destination_summary_stats_from_external_source(autopool, blocks, direction="in")
    return out_summary_stats_df, in_summary_stats_df


def _fetch_each_destination_tvl_df(autopool: AutopoolConstants, blocks: list[int]) -> pd.DataFrame:
    calls = _make_destination_tvl_calls(autopool)

    df = get_raw_state_by_blocks(calls, blocks, autopool.chain, include_block_number=True)
    for destination in get_destination_details(autopool):
        if destination.vaultAddress != autopool.autopool_eth_addr:
            df[f"{destination.vaultAddress}_TVL"] = df[f"{destination.vaultAddress}_totalSupply"] * (
                (df[f"{destination.vaultAddress}_floor"] + df[f"{destination.vaultAddress}_ceiling"]) / 2
            )

    # daily_df = df.resample("1D").last()
    # incentiveApr_df = fetch_destination_summary_stats(autopool, "incentiveApr").resample("1d").last()

    # for destination in get_destination_details(autopool):
    #     if destination.vaultAddress != autopool.autopool_eth_addr:
    #         daily_df[f"{destination.vaultAddress}_incentive_apr"] = incentiveApr_df[destination.vault_name]

    return df


def _fetch_all_vault_liquidated_events_df(autopool: AutopoolConstants) -> pd.DataFrame:

    lr_contract = autopool.chain.client.eth.contract(LIQUIDATION_ROW(autopool.chain), abi=LIQUIDATION_ROW_ABI)
    VaultLiquidated_df = add_timestamp_to_df_with_block_column(
        fetch_events(
            lr_contract.events.VaultLiquidated,
        ),
        autopool.chain,
    )
    # weth received for selling rewards tokens earned by this vault
    VaultLiquidated_df["weth"] = VaultLiquidated_df["amount"] / 1e18

    return VaultLiquidated_df


# def make_daily_data_df(autopool:AutopoolConstants) -> pd.DataFrame:
#     df = _fetch_each_destination_tvl_df(autopool)
#     VaultLiquidated_df = _fetch_all_vault_liquidated_events_df(autopool)
#     for destination in get_destination_details(autopool):
#         if destination.vaultAddress != autopool.autopool_eth_addr:

#             daily_incentive_token_sales = VaultLiquidated_df[VaultLiquidated_df['vault'].str.lower() == destination.vaultAddress.lower()].resample('1d')[['weth']].sum()
#             daily_df[f'{destination.vaultAddress}_daily_incentives_weth'] = daily_incentive_token_sales

#     return daily_df


def create_actual_and_expected_apr_df(
    autopool: AutopoolConstants,
    daily_df: pd.DataFrame,
    n_days_window: int,
    incentives_shift_days: int,
):
    names_to_vaults = {}

    for destination in get_destination_details(autopool):
        if (destination.vault_name not in names_to_vaults) and (destination.vaultAddress != autopool.autopool_eth_addr):
            names_to_vaults[destination.vault_name] = [destination.vaultAddress]
        else:
            names_to_vaults[destination.vault_name].append(destination.vaultAddress)

    def make_one_destination_incentive_apr_as_percent(destination_name: str, n: int):
        destination_vaults = names_to_vaults[destination_name]
        cols = [c for c in daily_df.columns if any((v in c for v in destination_vaults))]
        simple_df = pd.DataFrame(index=daily_df.index)
        tvl_cols = [c for c in cols if c[-4:] == "_TVL"]
        incentive_token_columns = [c for c in cols if "_daily_incentives_weth" in c]
        incentive_apr_columns = [c for c in cols if "_incentive_apr" in c]

        simple_df["TVL"] = daily_df[tvl_cols].sum(axis=1)

        # clip avg tvl to 0 if any of the days in the window have no tvl
        # todo break this apart into several functions instead of only one
        simple_df["rolling_avg_tvl"] = (
            simple_df["TVL"][::-1]
            .rolling(window=n, min_periods=n)
            .apply(lambda x: np.nan if (x == 0).any() else x.mean(), raw=True)[::-1]
        )

        simple_df["incentives_sold"] = daily_df[incentive_token_columns].sum(
            axis=1
        )  # daily weth value of incentives sold
        # use the total incentives earned in the days (incentives_shift_days,n + incentives_shift_days) when comparing the destination composite return out
        simple_df["rolling_sum_incentives"] = (
            simple_df["incentives_sold"][::-1].rolling(window=n, min_periods=n).sum()[::-1].shift(incentives_shift_days)
        )
        simple_df["summary_stats_incentive_apr_out_t0"] = 100 * daily_df[incentive_apr_columns].max(axis=1)

        simple_df["real_tvl_and_sold_incentives_apr"] = 100 * (
            (simple_df["rolling_sum_incentives"] / simple_df["rolling_avg_tvl"]) * (365 / n)
        )
        simple_df["destination"] = destination_name

        return simple_df

    simple_dfs = []
    for destination_name in names_to_vaults.keys():
        try:
            simple_df = make_one_destination_incentive_apr_as_percent(destination_name, n=n_days_window)
            simple_dfs.append(simple_df)

        except Exception as e:
            print(destination_name)

    expected_apr_df = pd.concat(simple_dfs)
    expected_apr_df["apr_diff"] = (
        expected_apr_df["real_tvl_and_sold_incentives_apr"] - expected_apr_df["summary_stats_incentive_apr_out_t0"]
    )

    expected_apr_df["WETH_not_earned"] = (
        (expected_apr_df["apr_diff"] / 100) * (n_days_window / 365)
    ) * expected_apr_df["rolling_avg_tvl"]
    return expected_apr_df


if __name__ == "__main__":
    from mainnet_launch.pages.solver_diagnostics.solver_diagnostics import _load_solver_df, AUTO_ETH

    calls = _make_destination_last_virtual_price_calls(AUTO_ETH)
