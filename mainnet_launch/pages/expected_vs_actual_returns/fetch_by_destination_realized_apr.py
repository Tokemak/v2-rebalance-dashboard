import pandas as pd
import streamlit as st
from multicall import Call

from mainnet_launch.abis import LIQUIDATION_ROW_ABI
from mainnet_launch.constants import (
    ROOT_PRICE_ORACLE,
    WETH,
    LIQUIDATION_ROW,
    AutopoolConstants,
    ALL_AUTOPOOLS,
)

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
from mainnet_launch.destinations import get_destination_details


from mainnet_launch.database.database_operations import (
    write_dataframe_to_table,
    get_earliest_block_from_table_with_autopool,
    get_all_rows_in_table_by_autopool,
)

from mainnet_launch.database.should_update_database import (
    should_update_table,
)


BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE = "BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE"


def add_new_destination_projected_and_actual_returns_to_table():

    if should_update_table(BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE):
        for autopool in ALL_AUTOPOOLS:
            highest_block_already_fetched = get_earliest_block_from_table_with_autopool(
                BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE, autopool
            )

            blocks = [b for b in build_blocks_to_use(autopool.chain) if b > highest_block_already_fetched]
            if len(blocks) > 0:
                df = _fetch_by_destination_actualized_apr_raw_data_from_external_source(autopool, min(blocks))
                df = df.reset_index()
                df["autopool"] = autopool.name
                write_dataframe_to_table(df, BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE)


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


def _build_onchain_expected_apr_components(
    autopool: AutopoolConstants, out_summary_stats_df: pd.DataFrame, in_summary_stats_df: pd.DataFrame
) -> pd.DataFrame:
    """Returns the perapr components from the summary stats"""
    dfs = []
    for col in ["baseApr", "feeApr", "incentiveApr"]:
        df = pd.pivot(out_summary_stats_df, columns="destination", values=col, index="block").reset_index()
        df = add_timestamp_to_df_with_block_column(df, autopool.chain).drop(columns=["block"])
        df = (
            df.resample("1D")
            .last()
            .reset_index()
            .melt(id_vars="timestamp", var_name="vault_name", value_name=col)
            .set_index(["timestamp", "vault_name"])
        )
        dfs.append(df)

    expected_apr_components_df = pd.concat(dfs, axis=1)
    expected_apr_components_df = expected_apr_components_df.rename(columns={"incentiveApr": "incentiveAprOut"})

    incentive_in_apr_df = (
        add_timestamp_to_df_with_block_column(
            pd.pivot(in_summary_stats_df, columns="destination", values="incentiveApr", index="block").reset_index(),
            autopool.chain,
        )
        .drop(columns=["block"])
        .resample("1D")
        .last()
        .reset_index()
        .melt(id_vars="timestamp", var_name="vault_name", value_name="incentiveAprIn")
        .set_index(["timestamp", "vault_name"])
    )

    expected_apr_components_df["incentiveAprIn"] = incentive_in_apr_df["incentiveAprIn"]
    expected_apr_components_df = expected_apr_components_df * 100
    return expected_apr_components_df


def _extract_raw_onchain_returns_df(
    autopool: AutopoolConstants,
    tvl_df: pd.DataFrame,
    last_snapshot_virtual_price_df: pd.DataFrame,
    VaultLiquidated_df: pd.DataFrame,
):
    # returns the TVL, virtual price, and incentives sold by day for each destination
    names_to_vaults = {}

    for destination in get_destination_details(autopool):
        if destination.vault_name not in names_to_vaults:
            names_to_vaults[destination.vault_name] = [destination.vaultAddress]
        else:
            names_to_vaults[destination.vault_name].append(destination.vaultAddress)

    def _extract_total_destination_tvl(tvl_df: pd.DataFrame) -> pd.DataFrame:
        by_destination_name_tvl_df = pd.DataFrame(index=tvl_df.index)
        for name, vaults in names_to_vaults.items():
            vaults_with_tvls = [f"{vault}_TVL" for vault in vaults if f"{vault}_TVL" in tvl_df.columns]
            by_destination_name_tvl_df[name] = tvl_df[vaults_with_tvls].sum(axis=1)
        return by_destination_name_tvl_df

    by_destination_name_tvl_df = (
        _extract_total_destination_tvl(tvl_df)
        .resample("1D")
        .last()
        .reset_index()
        .melt(id_vars="timestamp", var_name="vault_name", value_name="TVL")
        .set_index(["timestamp", "vault_name"])
    )

    def get_per_destination_virtual_price(last_snapshot_virtual_price_df: pd.DataFrame) -> pd.DataFrame:
        by_destination_name_virtual_price_df = pd.DataFrame(index=last_snapshot_virtual_price_df.index)
        for name, vaults in names_to_vaults.items():
            vaults_with_virtual_prices = [vault for vault in vaults if vault in last_snapshot_virtual_price_df.columns]
            by_destination_name_virtual_price_df[name] = last_snapshot_virtual_price_df[vaults_with_virtual_prices].max(
                axis=1
            )
        return by_destination_name_virtual_price_df

    by_destination_name_virtual_price_df = (
        get_per_destination_virtual_price(last_snapshot_virtual_price_df)
        .resample("1D")
        .last()
        .reset_index()
        .melt(id_vars="timestamp", var_name="vault_name", value_name="virtual_price")
        .set_index(["timestamp", "vault_name"])
    )

    def _extract_by_destination_incentives_sold(VaultLiquidated_df: pd.DataFrame) -> pd.DataFrame:
        by_destination_vault_liquidated_df = pd.DataFrame(index=VaultLiquidated_df.index).resample("1d").last()

        for name, vaults in names_to_vaults.items():
            this_vault_reward_liquidations = (
                VaultLiquidated_df[VaultLiquidated_df["vault"].isin(vaults)]["weth"].resample("1d").sum()
            )

            by_destination_vault_liquidated_df[name] = this_vault_reward_liquidations

        by_destination_vault_liquidated_df = by_destination_vault_liquidated_df.fillna(0)
        return by_destination_vault_liquidated_df

    by_destination_vault_liquidated_df = (
        _extract_by_destination_incentives_sold(VaultLiquidated_df)
        .reset_index()
        .melt(id_vars="timestamp", var_name="vault_name", value_name="incentives_sold")
        .set_index(["timestamp", "vault_name"])
    )

    timestamp_to_block = tvl_df[["block"]].resample("1D").last()

    raw_onchain_returns_df = pd.concat(
        [by_destination_name_tvl_df, by_destination_name_virtual_price_df, by_destination_vault_liquidated_df], axis=1
    )
    raw_onchain_returns_df = raw_onchain_returns_df.reset_index().set_index("timestamp")
    raw_onchain_returns_df = raw_onchain_returns_df.merge(timestamp_to_block, left_index=True, right_index=True)

    # avoid sql -> pandas float resolution errors
    raw_onchain_returns_df["virtual_price"] = pd.to_numeric(
        raw_onchain_returns_df["virtual_price"], errors="coerce"
    ).round(12)

    raw_onchain_returns_df = raw_onchain_returns_df.reset_index().set_index(["timestamp", "vault_name"])
    return raw_onchain_returns_df


def _set_base_apr_to_0_for_double_counting_destinations(long_df: pd.DataFrame) -> pd.DataFrame:
    # clip base apr to 0, because it is double counted in fee apr for these pools

    long_df = long_df.reset_index()
    double_counting_base_apr = ["wETHrETH (curve)", "ethx-f (curve)", "osETH-rETH (curve)", "weeth-ng (curve)"]
    long_df.loc[long_df["vault_name"].isin(double_counting_base_apr), "baseApr"] = 0.0
    long_df = long_df.set_index(["timestamp", "vault_name"])
    return long_df


def _fetch_by_destination_actualized_apr_raw_data_from_external_source(autopool: AutopoolConstants, start_block: int):

    blocks = build_blocks_to_use(
        autopool.chain, start_block=start_block, approx_num_blocks_per_day=1
    )  # TODO swich to 4
    out_summary_stats_df, in_summary_stats_df = _fetch_in_and_out_summary_stats(autopool, blocks)
    tvl_df = _fetch_each_destination_tvl_df(autopool, blocks)
    VaultLiquidated_df = _fetch_all_vault_liquidated_events_df(autopool)
    last_snapshot_virtual_price_df = _fetch_destination_last_virtual_price_df(autopool, blocks)
    expected_apr_components_df = _build_onchain_expected_apr_components(
        autopool, out_summary_stats_df, in_summary_stats_df
    )
    raw_onchain_returns_df = _extract_raw_onchain_returns_df(
        autopool, tvl_df, last_snapshot_virtual_price_df, VaultLiquidated_df
    )
    long_df = pd.concat([expected_apr_components_df, raw_onchain_returns_df], axis=1)
    long_df = long_df.sort_index()
    long_df = _set_base_apr_to_0_for_double_counting_destinations(long_df)

    return long_df


def fetch_by_destination_actualized_and_projected_apr(autopool: AutopoolConstants) -> pd.DataFrame:
    add_new_destination_projected_and_actual_returns_to_table()
    long_df = get_all_rows_in_table_by_autopool(BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE, autopool).reset_index()
    long_df = long_df.set_index(["timestamp", "vault_name"]).sort_index()
    return long_df


if __name__ == "__main__":
    from mainnet_launch.constants import  AUTO_ETH, BAL_ETH

    df = _fetch_by_destination_actualized_apr_raw_data_from_external_source(BAL_ETH,BAL_ETH.chain.block_autopool_first_deployed)
