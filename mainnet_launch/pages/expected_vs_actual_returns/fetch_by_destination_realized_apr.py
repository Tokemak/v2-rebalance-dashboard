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
    drop_table,
    run_read_only_query,
)
from mainnet_launch.database.should_update_database import should_update_table

BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE = "BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE"


# ─── TRANSFORMATION HELPERS ───────────────────────────────────────────────────────


def map_destination_names_to_vaults(autopool: AutopoolConstants) -> dict:
    """
    Creates a mapping from destination names to a list of their vault addresses.
    """
    names_to_vaults = {}
    for destination in get_destination_details(autopool):
        names_to_vaults.setdefault(destination.vault_name, []).append(destination.vaultAddress)
    return names_to_vaults


def extract_total_destination_tvl(tvl_df: pd.DataFrame, names_to_vaults: dict) -> pd.DataFrame:
    """
    For each destination (by name) sum the TVLs from all vaults.
    """
    # this is for when a destination is replaced
    by_destination_tvl = pd.DataFrame(index=tvl_df.index)
    for name, vaults in names_to_vaults.items():
        vaults_with_tvls = [f"{vault}_TVL" for vault in vaults if f"{vault}_TVL" in tvl_df.columns]
        by_destination_tvl[name] = tvl_df[vaults_with_tvls].sum(axis=1)
    # Resample and melt into a tidy format.
    by_destination_tvl = (
        by_destination_tvl.resample("1D")
        .last()
        .reset_index()
        .melt(id_vars="timestamp", var_name="vault_name", value_name="TVL")
        .set_index(["timestamp", "vault_name"])
    )
    return by_destination_tvl


def extract_destination_virtual_price(
    last_snapshot_virtual_price_df: pd.DataFrame, names_to_vaults: dict
) -> pd.DataFrame:
    """
    For each destination (by name) select the maximum virtual price across vaults.
    """
    by_destination_vprice = pd.DataFrame(index=last_snapshot_virtual_price_df.index)
    for name, vaults in names_to_vaults.items():
        vaults_with_vprice = [vault for vault in vaults if vault in last_snapshot_virtual_price_df.columns]
        # max here because if any of them are decaying then we want to know that
        # only the most recent is decaying
        # not certain on this part here
        by_destination_vprice[name] = last_snapshot_virtual_price_df[vaults_with_vprice].max(axis=1)
    by_destination_vprice = (
        by_destination_vprice.resample("1D")
        .last()
        .reset_index()
        .melt(id_vars="timestamp", var_name="vault_name", value_name="virtual_price")
        .set_index(["timestamp", "vault_name"])
    )
    return by_destination_vprice


def extract_destination_decay_state(decay_state_df: pd.DataFrame, names_to_vaults: dict) -> pd.DataFrame:
    """
    For each destination (by name) select the decay state.
    """
    by_destination_decay = pd.DataFrame(index=decay_state_df.index)
    for name, vaults in names_to_vaults.items():
        vaults_with_decay = [vault for vault in vaults if vault in decay_state_df.columns]
        by_destination_decay[name] = decay_state_df[vaults_with_decay].max(axis=1)
    by_destination_decay = (
        by_destination_decay.resample("1D")
        .last()
        .reset_index()
        .melt(id_vars="timestamp", var_name="vault_name", value_name="decay_state")
        .set_index(["timestamp", "vault_name"])
    )
    return by_destination_decay


def extract_incentives_sold(VaultLiquidated_df: pd.DataFrame, names_to_vaults: dict) -> pd.DataFrame:
    """
    Aggregates the 'weth' received (as incentives sold) per destination.
    """
    by_destination_incentives = pd.DataFrame(index=VaultLiquidated_df.index).resample("1D").last()
    for name, vaults in names_to_vaults.items():
        vault_liquidations = VaultLiquidated_df[VaultLiquidated_df["vault"].isin(vaults)]["weth"].resample("1D").sum()
        by_destination_incentives[name] = vault_liquidations
    by_destination_incentives = (
        by_destination_incentives.fillna(0)
        .reset_index()
        .melt(id_vars="timestamp", var_name="vault_name", value_name="incentives_sold")
        .set_index(["timestamp", "vault_name"])
    )
    return by_destination_incentives


def merge_onchain_returns_components(
    tvl_df: pd.DataFrame,
    tvl_comp: pd.DataFrame,
    vprice_comp: pd.DataFrame,
    incentives_comp: pd.DataFrame,
    decay_comp: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge the extracted components and add the block number (using TVL's block column).
    """
    merged = pd.concat([tvl_comp, vprice_comp, incentives_comp, decay_comp], axis=1)
    merged = merged.reset_index().set_index("timestamp")
    # Extract the block for each day.
    timestamp_to_block = tvl_df[["block"]].resample("1D").last()
    merged = merged.merge(timestamp_to_block, left_index=True, right_index=True)
    merged["virtual_price"] = pd.to_numeric(merged["virtual_price"], errors="coerce").round(12)
    merged = merged.reset_index().set_index(["timestamp", "vault_name"])
    return merged


def transform_exclude_autopool_and_convert_decay(df: pd.DataFrame, autopool: AutopoolConstants) -> pd.DataFrame:
    """
    Remove the autopool itself (since its returns are always 0) and convert the decay state
    column from booleans to an Int8 type (True -> 1, False -> 0, missing remains NA).
    """
    df = df.reset_index()
    df = df[df["vault_name"] != autopool.name]
    df = df.set_index(["timestamp", "vault_name"])
    return df


# ─── EXISTING TRANSFORMATIONS ─────────────────────────────────────────────────────


def _set_base_apr_to_0_for_double_counting_destinations(long_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clip base APR to 0 for destinations that double count APR in fee APR.
    """
    double_counting = [
        "wETHrETH (curve)",
        "ethx-f (curve)",
        "osETH-rETH (curve)",
        "weeth-ng (curve)",
    ]

    mask = long_df.index.get_level_values("vault_name").isin(double_counting)
    long_df.loc[mask, "baseApr"] = 0.0
    return long_df


# ─── RAW ON-CHAIN RETURNS EXTRACTION ─────────────────────────────────────────────


def extract_raw_onchain_returns_df(
    autopool: AutopoolConstants,
    tvl_df: pd.DataFrame,
    last_snapshot_virtual_price_df: pd.DataFrame,
    VaultLiquidated_df: pd.DataFrame,
    decay_state_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Extract and merge on-chain return components: TVL, virtual price, decay state, and incentives sold.
    """
    names_to_vaults = map_destination_names_to_vaults(autopool)
    tvl_comp = extract_total_destination_tvl(tvl_df, names_to_vaults)
    vprice_comp = extract_destination_virtual_price(last_snapshot_virtual_price_df, names_to_vaults)
    decay_comp = extract_destination_decay_state(decay_state_df, names_to_vaults)
    incentives_comp = extract_incentives_sold(VaultLiquidated_df, names_to_vaults)
    return merge_onchain_returns_components(tvl_df, tvl_comp, vprice_comp, incentives_comp, decay_comp)


# ─── FETCHING & FINAL TRANSFORMATIONS ─────────────────────────────────────────────


def fetch_stuff(autopool: AutopoolConstants, start_block: int) -> pd.DataFrame:
    """
    Fetch raw on-chain and expected APR data from external sources, merge them,
    and apply final transformations.
    """
    blocks = build_blocks_to_use(autopool.chain, start_block=start_block, approx_num_blocks_per_day=1)
    out_stats_df, in_stats_df = _fetch_in_and_out_summary_stats(autopool, blocks)
    tvl_df = _fetch_each_destination_tvl_df(autopool, blocks)
    VaultLiquidated_df = _fetch_all_vault_liquidated_events_df(autopool, max(blocks))

    last_snapshot_virtual_price_df, decay_state_df = _fetch_destination_last_virtual_price_and_decay_state(
        autopool, blocks
    )
    return blocks, out_stats_df, in_stats_df, tvl_df, VaultLiquidated_df, last_snapshot_virtual_price_df, decay_state_df


def _fetch_by_destination_actualized_apr_raw_data_from_external_source(
    autopool: AutopoolConstants, start_block: int
) -> pd.DataFrame:
    """
    Fetch raw on-chain and expected APR data from external sources, merge them,
    and apply final transformations.
    """
    blocks = build_blocks_to_use(autopool.chain, start_block=start_block)
    out_stats_df, in_stats_df = _fetch_in_and_out_summary_stats(autopool, blocks)
    tvl_df = _fetch_each_destination_tvl_df(autopool, blocks)
    VaultLiquidated_df = _fetch_all_vault_liquidated_events_df(autopool, max(blocks))

    last_snapshot_virtual_price_df, decay_state_df = _fetch_destination_last_virtual_price_and_decay_state(
        autopool, blocks
    )

    expected_apr_df = _build_onchain_expected_apr_components(autopool, out_stats_df, in_stats_df)
    raw_returns_df = extract_raw_onchain_returns_df(
        autopool, tvl_df, last_snapshot_virtual_price_df, VaultLiquidated_df, decay_state_df
    )

    combined_df = pd.concat([expected_apr_df, raw_returns_df], axis=1).sort_index()
    combined_df = _set_base_apr_to_0_for_double_counting_destinations(combined_df)
    # exclude the autopool as a destination
    combined_df = combined_df[~combined_df.index.get_level_values("vault_name").isin([f"{autopool.name} (tokemak)"])]
    combined_df["autopool"] = autopool.name
    combined_df = combined_df.reset_index().drop(columns=["timestamp"])
    # add back in the correct timestmap
    combined_df = add_timestamp_to_df_with_block_column(combined_df, autopool.chain).reset_index()
    return combined_df


def add_new_destination_projected_and_actual_returns_to_table():
    if should_update_table(BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE):
        for autopool in ALL_AUTOPOOLS:
            highest_block = get_earliest_block_from_table_with_autopool(
                BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE, autopool
            )
            blocks = [b for b in build_blocks_to_use(autopool.chain) if b > highest_block]
            if blocks:
                df = _fetch_by_destination_actualized_apr_raw_data_from_external_source(autopool, min(blocks))
                write_dataframe_to_table(df, BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE)


def fetch_by_destination_actualized_and_projected_apr(autopool: AutopoolConstants) -> pd.DataFrame:
    add_new_destination_projected_and_actual_returns_to_table()
    long_df = get_all_rows_in_table_by_autopool(BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE, autopool).reset_index()
    long_df = long_df.dropna(subset=["block"])
    # Use the highest block for each day when timestamp and vault_name are the same.
    long_df = long_df.loc[long_df.groupby(["timestamp", "vault_name"])["block"].idxmax()]
    long_df = long_df.set_index(["timestamp", "vault_name"]).sort_index()
    return long_df


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
            total_supply_call = Call(
                destination.vaultAddress,
                ["totalSupply()(uint256)"],
                [(f"{destination.vaultAddress}_totalSupply", safe_normalize_with_bool_success)],
            )
            calls.extend([floor_price_call, ceiling_price_call, total_supply_call])
    return calls


def _make_destination_virtual_price_and_decay_state_calls(autopool: AutopoolConstants) -> tuple[list[Call], list[Call]]:
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

    def _bool_to_int_or_None_with_bool_success(success: bool, x: bool):
        if success:
            if not isinstance(x, bool):
                raise ValueError("Expected a boolean")
            if x is True:
                return 1
            else:
                return 0

    decay_state_calls = [
        Call(
            stats,
            ["decayState()(bool)"],
            [(dest_vault_address, _bool_to_int_or_None_with_bool_success)],
        )
        for dest_vault_address, stats in dest_to_stats.items()
        if stats is not None
    ]
    return last_virtual_price_calls, decay_state_calls


def _fetch_destination_last_virtual_price_and_decay_state(autopool: AutopoolConstants, blocks: list[int]):
    last_virtual_price_calls, decay_state_calls = _make_destination_virtual_price_and_decay_state_calls(autopool)
    last_snapshot_virtual_price_df = get_raw_state_by_blocks(
        last_virtual_price_calls, blocks, autopool.chain, include_block_number=True
    )
    decay_state_df = get_raw_state_by_blocks(decay_state_calls, blocks, autopool.chain, include_block_number=True)
    return last_snapshot_virtual_price_df, decay_state_df


def _fetch_in_and_out_summary_stats(autopool: AutopoolConstants, blocks: list[int]):
    out_stats_df = _fetch_destination_summary_stats_from_external_source(autopool, blocks, direction="out")
    in_stats_df = _fetch_destination_summary_stats_from_external_source(autopool, blocks, direction="in")
    return out_stats_df, in_stats_df


def _fetch_each_destination_tvl_df(autopool: AutopoolConstants, blocks: list[int]) -> pd.DataFrame:
    calls = _make_destination_tvl_calls(autopool)
    df = get_raw_state_by_blocks(calls, blocks, autopool.chain, include_block_number=True)
    for destination in get_destination_details(autopool):
        if destination.vaultAddress != autopool.autopool_eth_addr:
            df[f"{destination.vaultAddress}_TVL"] = df[f"{destination.vaultAddress}_totalSupply"] * (
                (df[f"{destination.vaultAddress}_floor"] + df[f"{destination.vaultAddress}_ceiling"]) / 2
            )
    return df


def _fetch_all_vault_liquidated_events_df(autopool: AutopoolConstants, highest_block_to_fetch: int) -> pd.DataFrame:
    lr_contract = autopool.chain.client.eth.contract(LIQUIDATION_ROW(autopool.chain), abi=LIQUIDATION_ROW_ABI)
    VaultLiquidated_df = add_timestamp_to_df_with_block_column(
        fetch_events(lr_contract.events.VaultLiquidated, autopool.chain, end_block=highest_block_to_fetch),
        autopool.chain,
    )
    VaultLiquidated_df["weth"] = VaultLiquidated_df["amount"] / 1e18
    return VaultLiquidated_df


def _build_onchain_expected_apr_components(
    autopool: AutopoolConstants, out_summary_stats_df: pd.DataFrame, in_summary_stats_df: pd.DataFrame
) -> pd.DataFrame:
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
    expected_apr_df = pd.concat(dfs, axis=1)
    expected_apr_df = expected_apr_df.rename(columns={"incentiveApr": "incentiveAprOut"})
    incentive_in_df = (
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
    expected_apr_df["incentiveAprIn"] = incentive_in_df["incentiveAprIn"]
    return expected_apr_df * 100


# ─── MAIN ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from mainnet_launch.constants import AUTO_ETH, BAL_ETH, WORKING_DATA_DIR, AUTO_LRT, DINERO_ETH

    # drop_table(BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE)
    df = fetch_by_destination_actualized_and_projected_apr(DINERO_ETH)
    # print(df.shape)


# import pandas as pd
# import streamlit as st
# from multicall import Call

# from mainnet_launch.abis import LIQUIDATION_ROW_ABI
# from mainnet_launch.constants import (
#     ROOT_PRICE_ORACLE,
#     WETH,
#     LIQUIDATION_ROW,
#     AutopoolConstants,
#     ALL_AUTOPOOLS,
# )

# from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import (
#     _fetch_destination_summary_stats_from_external_source,
# )
# from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column

# from mainnet_launch.data_fetching.get_events import fetch_events

# from mainnet_launch.data_fetching.get_state_by_block import (
#     build_blocks_to_use,
#     get_raw_state_by_blocks,
#     get_state_by_one_block,
#     safe_normalize_with_bool_success,
#     identity_with_bool_success,
# )
# from mainnet_launch.destinations import get_destination_details


# from mainnet_launch.database.database_operations import (
#     write_dataframe_to_table,
#     get_earliest_block_from_table_with_autopool,
#     get_all_rows_in_table_by_autopool,
#     drop_table,
# )

# from mainnet_launch.database.should_update_database import (
#     should_update_table,
# )


# BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE = "BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE"


# def add_new_destination_projected_and_actual_returns_to_table():

#     if should_update_table(BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE):
#         for autopool in ALL_AUTOPOOLS:
#             highest_block_already_fetched = get_earliest_block_from_table_with_autopool(
#                 BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE, autopool
#             )

#             blocks = [b for b in build_blocks_to_use(autopool.chain) if b > highest_block_already_fetched]
#             if len(blocks) > 0:
#                 df = _fetch_by_destination_actualized_apr_raw_data_from_external_source(autopool, min(blocks))
#                 df = df.reset_index()
#                 df["autopool"] = autopool.name
#                 write_dataframe_to_table(df, BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE)


# def _make_destination_tvl_calls(autopool: AutopoolConstants) -> list[Call]:
#     calls = []
#     for destination in get_destination_details(autopool):
#         if destination.vaultAddress != autopool.autopool_eth_addr:
#             floor_price_call = Call(
#                 ROOT_PRICE_ORACLE(destination.autopool.chain),
#                 [
#                     "getFloorCeilingPrice(address,address,address,bool)(uint256)",
#                     destination.dexPool,
#                     destination.lpTokenAddress,
#                     WETH(destination.autopool.chain),
#                     False,
#                 ],
#                 [(f"{destination.vaultAddress}_floor", safe_normalize_with_bool_success)],
#             )
#             ceiling_price_call = Call(
#                 ROOT_PRICE_ORACLE(destination.autopool.chain),
#                 [
#                     "getFloorCeilingPrice(address,address,address,bool)(uint256)",
#                     destination.dexPool,
#                     destination.lpTokenAddress,
#                     WETH(destination.autopool.chain),
#                     True,
#                 ],
#                 [(f"{destination.vaultAddress}_ceiling", safe_normalize_with_bool_success)],
#             )
#             destination_total_supply_call = Call(
#                 destination.vaultAddress,
#                 ["totalSupply()(uint256)"],
#                 [(f"{destination.vaultAddress}_totalSupply", safe_normalize_with_bool_success)],
#             )

#             calls.extend([floor_price_call, ceiling_price_call, destination_total_supply_call])

#     return calls


# def _make_destination_virtual_price_and_decay_state_calls(autopool: AutopoolConstants) -> tuple[list[Call], list[Call]]:
#     # call destinationVaultAddress.stats() -> then destination underlyingStats() then call( lastVirtualPrice(), decayState())

#     destinations = get_destination_details(autopool)
#     get_stats_calls = [
#         Call(
#             dest.vaultAddress,
#             ["getStats()(address)"],
#             [(dest.vaultAddress, identity_with_bool_success)],
#         )
#         for dest in destinations
#     ]

#     dest_to_stats = get_state_by_one_block(get_stats_calls, autopool.chain.client.eth.block_number, autopool.chain)
#     # dest:stats dict

#     underlyingStats_calls = [
#         Call(
#             stats,
#             ["underlyerStats()(address)"],
#             [(dest_vault_address, identity_with_bool_success)],
#         )
#         for dest_vault_address, stats in dest_to_stats.items()
#         if stats is not None
#     ]

#     dest_to_underlying_stats = get_state_by_one_block(
#         underlyingStats_calls, autopool.chain.client.eth.block_number, autopool.chain
#     )

#     last_virtual_price_calls = [
#         Call(
#             underlying_stats,
#             ["lastVirtualPrice()(uint256)"],
#             [(dest_vault_address, safe_normalize_with_bool_success)],
#         )
#         for dest_vault_address, underlying_stats in dest_to_underlying_stats.items()
#         if underlying_stats is not None
#     ]

#     decay_state_calls = [
#         Call(
#             stats,
#             ["decayState()(bool)"],
#             [(dest_vault_address, identity_with_bool_success)],
#         )
#         for dest_vault_address, stats in dest_to_stats.items()
#         if stats is not None
#     ]

#     return last_virtual_price_calls, decay_state_calls


# def _fetch_destination_last_virtual_price_and_decay_state(autopool: AutopoolConstants, blocks: list[int]):
#     last_virtual_price_calls, decay_state_calls = _make_destination_virtual_price_and_decay_state_calls(autopool)
#     last_snapshot_virtual_price_df = get_raw_state_by_blocks(
#         last_virtual_price_calls, blocks, autopool.chain, include_block_number=True
#     )

#     decay_state_df = get_raw_state_by_blocks(decay_state_calls, blocks, autopool.chain, include_block_number=True)
#     return last_snapshot_virtual_price_df, decay_state_df


# def _fetch_in_and_out_summary_stats(autopool: AutopoolConstants, blocks: list[int]):
#     out_summary_stats_df = _fetch_destination_summary_stats_from_external_source(autopool, blocks, direction="out")
#     in_summary_stats_df = _fetch_destination_summary_stats_from_external_source(autopool, blocks, direction="in")
#     return out_summary_stats_df, in_summary_stats_df


# def _fetch_each_destination_tvl_df(autopool: AutopoolConstants, blocks: list[int]) -> pd.DataFrame:
#     calls = _make_destination_tvl_calls(autopool)

#     df = get_raw_state_by_blocks(calls, blocks, autopool.chain, include_block_number=True)
#     for destination in get_destination_details(autopool):
#         if destination.vaultAddress != autopool.autopool_eth_addr:
#             df[f"{destination.vaultAddress}_TVL"] = df[f"{destination.vaultAddress}_totalSupply"] * (
#                 (df[f"{destination.vaultAddress}_floor"] + df[f"{destination.vaultAddress}_ceiling"]) / 2
#             )

#     return df


# def _fetch_all_vault_liquidated_events_df(autopool: AutopoolConstants) -> pd.DataFrame:

#     lr_contract = autopool.chain.client.eth.contract(LIQUIDATION_ROW(autopool.chain), abi=LIQUIDATION_ROW_ABI)
#     VaultLiquidated_df = add_timestamp_to_df_with_block_column(
#         fetch_events(
#             lr_contract.events.VaultLiquidated,
#         ),
#         autopool.chain,
#     )
#     # weth received for selling rewards tokens earned by this vault
#     VaultLiquidated_df["weth"] = VaultLiquidated_df["amount"] / 1e18

#     return VaultLiquidated_df


# def _build_onchain_expected_apr_components(
#     autopool: AutopoolConstants, out_summary_stats_df: pd.DataFrame, in_summary_stats_df: pd.DataFrame
# ) -> pd.DataFrame:
#     """Returns the perapr components from the summary stats"""
#     dfs = []
#     for col in ["baseApr", "feeApr", "incentiveApr"]:
#         df = pd.pivot(out_summary_stats_df, columns="destination", values=col, index="block").reset_index()
#         df = add_timestamp_to_df_with_block_column(df, autopool.chain).drop(columns=["block"])
#         df = (
#             df.resample("1D")
#             .last()
#             .reset_index()
#             .melt(id_vars="timestamp", var_name="vault_name", value_name=col)
#             .set_index(["timestamp", "vault_name"])
#         )
#         dfs.append(df)

#     expected_apr_components_df = pd.concat(dfs, axis=1)
#     expected_apr_components_df = expected_apr_components_df.rename(columns={"incentiveApr": "incentiveAprOut"})

#     incentive_in_apr_df = (
#         add_timestamp_to_df_with_block_column(
#             pd.pivot(in_summary_stats_df, columns="destination", values="incentiveApr", index="block").reset_index(),
#             autopool.chain,
#         )
#         .drop(columns=["block"])
#         .resample("1D")
#         .last()
#         .reset_index()
#         .melt(id_vars="timestamp", var_name="vault_name", value_name="incentiveAprIn")
#         .set_index(["timestamp", "vault_name"])
#     )

#     expected_apr_components_df["incentiveAprIn"] = incentive_in_apr_df["incentiveAprIn"]
#     expected_apr_components_df = expected_apr_components_df * 100
#     return expected_apr_components_df


# def _extract_raw_onchain_returns_df(
#     autopool: AutopoolConstants,
#     tvl_df: pd.DataFrame,
#     last_snapshot_virtual_price_df: pd.DataFrame,
#     VaultLiquidated_df: pd.DataFrame,
#     decay_state_df: pd.DataFrame,
# ):
#     # returns the TVL, virtual price, and incentives sold by day for each destination
#     names_to_vaults = {}

#     for destination in get_destination_details(autopool):
#         if destination.vault_name not in names_to_vaults:
#             names_to_vaults[destination.vault_name] = [destination.vaultAddress]
#         else:
#             names_to_vaults[destination.vault_name].append(destination.vaultAddress)

#     def _extract_total_destination_tvl(tvl_df: pd.DataFrame) -> pd.DataFrame:
#         by_destination_name_tvl_df = pd.DataFrame(index=tvl_df.index)
#         for name, vaults in names_to_vaults.items():
#             vaults_with_tvls = [f"{vault}_TVL" for vault in vaults if f"{vault}_TVL" in tvl_df.columns]
#             by_destination_name_tvl_df[name] = tvl_df[vaults_with_tvls].sum(axis=1)
#         return by_destination_name_tvl_df

#     by_destination_name_tvl_df = (
#         _extract_total_destination_tvl(tvl_df)
#         .resample("1D")
#         .last()
#         .reset_index()
#         .melt(id_vars="timestamp", var_name="vault_name", value_name="TVL")
#         .set_index(["timestamp", "vault_name"])
#     )

#     def get_per_destination_virtual_price(last_snapshot_virtual_price_df: pd.DataFrame) -> pd.DataFrame:
#         by_destination_name_virtual_price_df = pd.DataFrame(index=last_snapshot_virtual_price_df.index)
#         for name, vaults in names_to_vaults.items():
#             vaults_with_virtual_prices = [vault for vault in vaults if vault in last_snapshot_virtual_price_df.columns]
#             by_destination_name_virtual_price_df[name] = last_snapshot_virtual_price_df[vaults_with_virtual_prices].max(
#                 axis=1
#             )
#         return by_destination_name_virtual_price_df

#     by_destination_name_virtual_price_df = (
#         get_per_destination_virtual_price(last_snapshot_virtual_price_df)
#         .resample("1D")
#         .last()
#         .reset_index()
#         .melt(id_vars="timestamp", var_name="vault_name", value_name="virtual_price")
#         .set_index(["timestamp", "vault_name"])
#     )

#     def get_per_destination_decay_state(decay_state_df: pd.DataFrame) -> pd.DataFrame:
#         by_destination_decay_state = pd.DataFrame(index=decay_state_df.index)
#         for name, vaults in names_to_vaults.items():
#             vaults_with_decay_state = [vault for vault in vaults if vault in decay_state_df.columns]
#             by_destination_decay_state[name] = decay_state_df[vaults_with_decay_state].max(axis=1)
#         return by_destination_decay_state

#     by_destination_name_decay_state_df = (
#         get_per_destination_decay_state(decay_state_df)
#         .resample("1D")
#         .last()
#         .reset_index()
#         .melt(id_vars="timestamp", var_name="vault_name", value_name="decay_state")
#         .set_index(["timestamp", "vault_name"])
#     )

#     def _extract_by_destination_incentives_sold(VaultLiquidated_df: pd.DataFrame) -> pd.DataFrame:
#         by_destination_vault_liquidated_df = pd.DataFrame(index=VaultLiquidated_df.index).resample("1d").last()

#         for name, vaults in names_to_vaults.items():
#             this_vault_reward_liquidations = (
#                 VaultLiquidated_df[VaultLiquidated_df["vault"].isin(vaults)]["weth"].resample("1d").sum()
#             )

#             by_destination_vault_liquidated_df[name] = this_vault_reward_liquidations

#         by_destination_vault_liquidated_df = by_destination_vault_liquidated_df.fillna(0)
#         return by_destination_vault_liquidated_df

#     by_destination_vault_liquidated_df = (
#         _extract_by_destination_incentives_sold(VaultLiquidated_df)
#         .reset_index()
#         .melt(id_vars="timestamp", var_name="vault_name", value_name="incentives_sold")
#         .set_index(["timestamp", "vault_name"])
#     )

#     timestamp_to_block = tvl_df[["block"]].resample("1D").last()

#     raw_onchain_returns_df = pd.concat(
#         [
#             by_destination_name_tvl_df,
#             by_destination_name_virtual_price_df,
#             by_destination_vault_liquidated_df,
#             by_destination_name_decay_state_df,
#         ],
#         axis=1,
#     )
#     raw_onchain_returns_df = raw_onchain_returns_df.reset_index().set_index("timestamp")
#     raw_onchain_returns_df = raw_onchain_returns_df.merge(timestamp_to_block, left_index=True, right_index=True)

#     # avoid sql -> pandas float resolution errors
#     raw_onchain_returns_df["virtual_price"] = pd.to_numeric(
#         raw_onchain_returns_df["virtual_price"], errors="coerce"
#     ).round(12)

#     raw_onchain_returns_df = raw_onchain_returns_df.reset_index().set_index(["timestamp", "vault_name"])
#     return raw_onchain_returns_df


# def _set_base_apr_to_0_for_double_counting_destinations(long_df: pd.DataFrame) -> pd.DataFrame:
#     # clip base apr to 0, because it is double counted in fee apr for these pools

#     long_df = long_df.reset_index()
#     double_counting_base_apr = [
#         "wETHrETH (curve)",
#         "ethx-f (curve)",
#         "osETH-rETH (curve)",
#         "weeth-ng (curve)",
#     ]  # are there more than 2 alive
#     long_df.loc[long_df["vault_name"].isin(double_counting_base_apr), "baseApr"] = 0.0
#     long_df = long_df.set_index(["timestamp", "vault_name"])
#     return long_df


# def _fetch_by_destination_actualized_apr_raw_data_from_external_source(autopool: AutopoolConstants, start_block: int):

#     blocks = build_blocks_to_use(
#         autopool.chain, start_block=start_block, approx_num_blocks_per_day=1
#     )  # TODO swich to 4
#     out_summary_stats_df, in_summary_stats_df = _fetch_in_and_out_summary_stats(autopool, blocks)
#     tvl_df = _fetch_each_destination_tvl_df(autopool, blocks)
#     VaultLiquidated_df = _fetch_all_vault_liquidated_events_df(autopool)
#     last_snapshot_virtual_price_df, decay_state_df = _fetch_destination_last_virtual_price_and_decay_state(
#         autopool, blocks
#     )
#     expected_apr_components_df = _build_onchain_expected_apr_components(
#         autopool, out_summary_stats_df, in_summary_stats_df
#     )
#     raw_onchain_returns_df = _extract_raw_onchain_returns_df(
#         autopool, tvl_df, last_snapshot_virtual_price_df, VaultLiquidated_df, decay_state_df
#     )
#     long_df = pd.concat([expected_apr_components_df, raw_onchain_returns_df], axis=1)
#     long_df = long_df.sort_index()
#     long_df = _set_base_apr_to_0_for_double_counting_destinations(long_df)

#     long_df = long_df.reset_index()

#     # exclude the autopool itself because it's expected and actual return
#     # is always 0% because it is plain base asset

#     long_df = long_df[long_df["vault_name"] != autopool.name]
#     long_df = long_df.set_index(["timestamp", "vault_name"])
#     long_df["decay_state"] = (
#         long_df["decay_state"].apply(lambda x: 1 if x is True else 0 if x is False else pd.NA).astype("Int8")
#     )

#     return long_df


# def fetch_by_destination_actualized_and_projected_apr(autopool: AutopoolConstants) -> pd.DataFrame:
#     add_new_destination_projected_and_actual_returns_to_table()
#     long_df = get_all_rows_in_table_by_autopool(BY_DESTINATION_PROJECTED_AND_EXPECTED_APR_TABLE, autopool).reset_index()
#     long_df = long_df.dropna(subset=["block"])
#     # use the highest block for each day as the data set
#     # when timestamp, and vault address are the same
#     # keep the row that has the highest block
#     # this can happen because if we run this again on different days, and we are only keeping the last block per day

#     long_df = long_df.loc[long_df.groupby(["timestamp", "vault_name"])["block"].idxmax()]
#     long_df = long_df.set_index(["timestamp", "vault_name"]).sort_index()
#     return long_df
