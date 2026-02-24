from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    build_blocks_to_use,
    get_raw_state_by_blocks,
    safe_normalize_6_with_bool_success,
    safe_normalize_with_bool_success,
    identity_with_bool_success,
    make_dummy_1_call,
)
from multicall import Call

from mainnet_launch.database.postgres_operations import (
    get_full_table_as_df,
    get_full_table_as_df_with_tx_hash,
)
from mainnet_launch.database.schema.full import (
    RebalanceEvents,
    RebalancePlans,
    Blocks,
)

from mainnet_launch.data_fetching.defi_llama.fetch_timestamp import fetch_blocks_by_unix_timestamps_defillama

from mainnet_launch.constants import (
    WORKING_DATA_DIR,
    AutopoolConstants,
    ChainData,
    AUTO_ETH,
    BASE_ETH,
    AUTO_LRT,
    BAL_ETH,
    DINERO_ETH,
)
from mainnet_launch.data_fetching.internal.s3_helper import fetch_rebalance_plan_json_no_s3_client
from mainnet_launch.pages.autopool.autopool_diagnostics.lens_contract import build_proxyGetDestinationSummaryStats_call

from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm.auto import tqdm

import json
import numpy as np
import os
import pandas as pd
import plotly.express as px
import plotly.io as pio


pio.templates.default = None

import json
import shutil


def compute_apr(vp_df: pd.DataFrame) -> pd.DataFrame:

    t0 = vp_df.index[0]
    days = (vp_df.index - t0).total_seconds() / 86400.0

    out0 = vp_df["out_vp"].iloc[0]
    in0 = vp_df["in_vp"].iloc[0]

    # annualized % using actual elapsed days; guard day=0 at start
    vp_df["out_ann_pct"] = np.where(days > 0, ((vp_df["out_vp"] / out0) ** (365.0 / days) - 1.0), np.nan)
    vp_df["in_ann_pct"] = np.where(days > 0, ((vp_df["in_vp"] / in0) ** (365.0 / days) - 1.0), np.nan)

    return vp_df[["block", "out_vp", "in_vp", "out_ann_pct", "in_ann_pct"]]


def compute_aerodome_vp(destination_vault_address: str, block: int, chain: ChainData) -> float:
    pool = "0x91F0f34916Ca4E2cCe120116774b0e4fA0cdcaA8"
    getK_k_call = Call(pool, "getK()(uint256)", [("K", safe_normalize_with_bool_success)])

    get_total_supply_call = Call(pool, "totalSupply()(uint256)", [("total_supply", safe_normalize_with_bool_success)])

    state = get_state_by_one_block([getK_k_call, get_total_supply_call], block, chain)

    vp = state["K"] / state["total_supply"]
    return vp


def build_dv_to_sig_to_vp(autopool: AutopoolConstants):
    events = get_full_table_as_df_with_tx_hash(
        RebalanceEvents, where_clause=RebalanceEvents.autopool_vault_address == autopool.autopool_eth_addr
    )
    destinations = set(events["destination_out"].unique().tolist() + events["destination_in"].unique().tolist())
    calls = [Call(address, "getStats()(address)", [(address, identity_with_bool_success)]) for address in destinations]

    destination_vault_address_to_stats_contract = get_state_by_one_block(
        calls, autopool.chain.get_block_near_top(), autopool.chain
    )

    calls = [
        Call(stats_contract, "underlyerStats()(address)", [(destination_vault, identity_with_bool_success)])
        for destination_vault, stats_contract in destination_vault_address_to_stats_contract.items()
        if stats_contract is not None
    ]

    # destination_vault_to_underlyer = get_state_by_one_block(calls, autopool.chain.get_block_near_top(), autopool.chain)

    calls = [
        Call(stats_contract, "pool()(address)", [(destination_vault, identity_with_bool_success)])
        for destination_vault, stats_contract in destination_vault_address_to_stats_contract.items()
        if stats_contract is not None
    ]
    destination_vault_to_pool = get_state_by_one_block(calls, autopool.chain.get_block_near_top(), autopool.chain)

    # aredrom is getK() / totalSupply() instead of get_virtual_price()

    destination_vault_to_pool["0x3772973f8F399D74488D5cF3276C032E0afC8A6f"] = (
        "0x94B17476A93b3262d87B9a326965D1E91f9c13E7"  # curvePool()(address)
    )
    destination_vault_to_pool["0xe4433D00Cf48BFE0C672d9949F2cd2c008bffC04"] = (
        "0x6951bDC4734b9f7F3E1B74afeBC670c736A0EDB6"  # curvePool()(address)
    )
    destination_vault_to_pool["0xc4Eb861e7b66f593482a3D7E8adc314f6eEDA30B"] = (
        "0x88794C65550DeB6b4087B7552eCf295113794410"  # balancerPool()(address)
    )
    destination_vault_to_pool["0x2C7120dCCF1c14A37A26A4955475d45d34a3d7E7"] = (
        "0xA0D3707c569ff8C87FA923d3823eC5D81c98Be78"  # getpool instadapp ETHv2
    )
    destination_vault_to_pool["0xd100c932801390fdeBcE11F26f611D4898b44236"] = (
        "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"  # getPool wstETH (holding)
    )
    destination_vault_to_pool["0x945a4f719018edBa445ca67bDa43663C815835Ad"] = (
        "0x91F0f34916Ca4E2cCe120116774b0e4fA0cdcaA8"  # getPool aerodrome weETH/WETH
    )

    destination_vault_to_pool["0x896eCc16Ab4AFfF6cE0765A5B924BaECd7Fa455a"] = "0xe080027Bd47353b5D1639772b4a75E9Ed3658A0d"
    destination_vault_to_pool["0xd96E943098B2AE81155e98D7DC8BeaB34C539f01"] = "0x6951bDC4734b9f7F3E1B74afeBC670c736A0EDB6"
    destination_vault_to_pool["0xE382BBd32C4E202185762eA433278f4ED9E6151E"] = "0xC8Eb2Cf2f792F77AF0Cd9e203305a585E588179D"
    destination_vault_to_pool["0xfB6f99FdF12E37Bfe3c4Cf81067faB10c465fb24"] = "0xB91159aa527D4769CB9FAf3e4ADB760c7E8C8Ea7"
    destination_vault_to_pool["0xC001f23397dB71B17602Ce7D90a983Edc38DB0d1"] = "0x59Ab5a5b5d617E478a2479B0cAD80DA7e2831492"
    destination_vault_to_pool["0x8cA2201BC34780f14Bca452913ecAc8e9928d4cA"] = "0x88794C65550DeB6b4087B7552eCf295113794410"

    function_signatures = [
        "get_virtual_price()(uint256)",
        "getRate()(uint256)",
        "stEthPerToken()(uint256)",
        "exchangePrice()(uint256)",
        "getInvariantDivActualSupply()(uint256)",  # Gyroscope ECLP pools
    ]

    # Build calls to get virtual price for each pool
    vp_calls = []
    for destination_vault, pool_address in destination_vault_to_pool.items():
        for function_signature in function_signatures:
            vp_calls.append(
                Call(
                    pool_address,
                    function_signature,
                    [((destination_vault, pool_address, function_signature), safe_normalize_with_bool_success)],
                )
            )

    virtual_prices = get_state_by_one_block(vp_calls, autopool.chain.get_block_near_top(), autopool.chain)
    vp_df = pd.DataFrame.from_dict(virtual_prices, orient="index", columns=["virtual_price"]).reset_index()
    vp_df[["destination_vault", "pool", "function_signature"]] = pd.DataFrame(
        vp_df["index"].tolist(), index=vp_df.index
    )

    # destination_vault -> {function_signature: virtual_price_or_None}
    dv_to_sig_to_vp = {}
    for _, r in vp_df.iterrows():
        dv = r["destination_vault"]
        pool = r["pool"]
        sig = r["function_signature"]
        v = r["virtual_price"]
        if pd.notna(v):
            dv_to_sig_to_vp[dv] = (pool, sig)

    return dv_to_sig_to_vp


# # Find destination vaults that don't have a valid virtual price mapping
# destinations_without_vp = destinations - set(dv_to_sig_to_vp.keys())
# print(f"Destination vaults without virtual price: {len(destinations_without_vp)}")
# print(destinations_without_vp)


def build_vp_call_from_destination_vault(dv_to_sig_to_vp, destination_vault, dir):
    if destination_vault not in dv_to_sig_to_vp:
        return make_dummy_1_call(f"{dir} virtual_price")
    pool, sig = dv_to_sig_to_vp[destination_vault]
    return Call(pool, sig, [(f"{dir} virtual_price", safe_normalize_with_bool_success)])


AERODROME_WEETH_WETH_DV = "0x945a4f719018edBa445ca67bDa43663C815835Ad"


def _override_aerodrome_vp(plan_data: dict, rebalance_block: int, block_30_days_in_future: int, chain: ChainData):
    """Replace VP for the Aerodrome weETH/WETH destination with K/totalSupply at both blocks."""
    if plan_data["destinationOut"] == AERODROME_WEETH_WETH_DV:
        plan_data["start_vp"]["out virtual_price"] = compute_aerodome_vp(plan_data["destinationOut"], rebalance_block, chain)
        plan_data["end_vp"]["out virtual_price"] = compute_aerodome_vp(plan_data["destinationOut"], block_30_days_in_future, chain)
    elif plan_data["destinationIn"] == AERODROME_WEETH_WETH_DV:
        plan_data["start_vp"]["in virtual_price"] = compute_aerodome_vp(plan_data["destinationIn"], rebalance_block, chain)
        plan_data["end_vp"]["in virtual_price"] = compute_aerodome_vp(plan_data["destinationIn"], block_30_days_in_future, chain)


def _compute_fee_and_base_apr(plan_data: dict, start_timestamp: int, end_timestamp: int):
    """Compute annualized fee+base APR from VP change, using actual elapsed time for annualization."""
    elapsed_days = (end_timestamp - start_timestamp) / 86400.0

    start_out_vp = plan_data["start_vp"]["out virtual_price"]
    end_out_vp = plan_data["end_vp"]["out virtual_price"]
    start_in_vp = plan_data["start_vp"]["in virtual_price"]
    end_in_vp = plan_data["end_vp"]["in virtual_price"]

    if start_out_vp > 0.01 and end_out_vp > 0.01:
        total_return_out = end_out_vp / start_out_vp - 1
        plan_data["out_fee_and_base"] = 100 * ((1 + total_return_out) ** (365.0 / elapsed_days) - 1)
    else:
        plan_data["out_fee_and_base"] = None

    if start_in_vp > 0.01 and end_in_vp > 0.01:
        total_return_in = end_in_vp / start_in_vp - 1
        plan_data["in_fee_and_base"] = 100 * ((1 + total_return_in) ** (365.0 / elapsed_days) - 1)
    else:
        plan_data["in_fee_and_base"] = None


class PlanVerificationError(Exception):
    pass


def _validate_plan(plan_data: dict):
    """Raise a error if the plan is malformed"""
    if plan_data["destinationOut"] is None:
        raise PlanVerificationError("destinationOut is None")
    if plan_data["destinationIn"] is None:
        raise PlanVerificationError("destinationIn is None")


def add_destination_summary_stats(dv_to_sig_to_vp: dict, plan_file_path: str, rebalance_block: int, autopool):
    plan_data = fetch_rebalance_plan_json_no_s3_client(plan_file_path, autopool)
    _validate_plan(plan_data)
    destination_out_summary_stats_call = build_proxyGetDestinationSummaryStats_call(
        "out", autopool, plan_data["destinationOut"], direction="out", amount=0
    )
    destination_in_summary_stats_call = build_proxyGetDestinationSummaryStats_call(
        "in", autopool, plan_data["destinationIn"], direction="in", amount=0
    )

    stats_at_block = get_state_by_one_block(
        [destination_out_summary_stats_call, destination_in_summary_stats_call], rebalance_block, autopool.chain
    )
    plan_data["destinationOutSummaryStats"] = stats_at_block
    out_vp_call = build_vp_call_from_destination_vault(dv_to_sig_to_vp, plan_data["destinationOut"], "out")
    in_vp_call = build_vp_call_from_destination_vault(dv_to_sig_to_vp, plan_data["destinationIn"], "in")

    start_vp = get_state_by_one_block([out_vp_call, in_vp_call], rebalance_block, autopool.chain)
    plan_data["start_vp"] = start_vp

    reblance_block_timestamp = autopool.chain.client.eth.get_block(rebalance_block)["timestamp"]
    current_timestamp = autopool.chain.client.eth.get_block("latest")["timestamp"]

    if reblance_block_timestamp + 30 * 24 * 3600 > current_timestamp:
        end_vp = {}
        block_30_days_in_future = None
    else:
        block_30_days_in_future = fetch_blocks_by_unix_timestamps_defillama(
            [reblance_block_timestamp + 30 * 24 * 3600], autopool.chain
        )[0]

        end_vp = get_state_by_one_block([out_vp_call, in_vp_call], block_30_days_in_future, autopool.chain)

    plan_data["end_vp"] = end_vp
    plan_data["block_30_days_in_future"] = block_30_days_in_future

    if end_vp != {}:
        _override_aerodrome_vp(plan_data, rebalance_block, block_30_days_in_future, autopool.chain)

        end_block_timestamp = autopool.chain.client.eth.get_block(block_30_days_in_future)["timestamp"]
        _compute_fee_and_base_apr(plan_data, reblance_block_timestamp, end_block_timestamp)

    else:
        plan_data["out_fee_and_base"] = None
        plan_data["in_fee_and_base"] = None
    return plan_data


def fetch_and_augment_onchain_calc_plans(autopool: AutopoolConstants) -> dict:
    AUGMENTED_PLANS_SAVE_DIR = WORKING_DATA_DIR / f"{autopool.name}_augmented_plans"
    os.makedirs(AUGMENTED_PLANS_SAVE_DIR, exist_ok=True)

    events = get_full_table_as_df_with_tx_hash(
        RebalanceEvents, where_clause=RebalanceEvents.autopool_vault_address == autopool.autopool_eth_addr
    )

    dv_to_sig_to_vp = build_dv_to_sig_to_vp(autopool)

    def fetch_and_save_augmented_plans(row):
        file_path = row["rebalance_file_path"]
        block = row["block"]
        augmented_plan = add_destination_summary_stats(dv_to_sig_to_vp, file_path, block, autopool)

        if augmented_plan["end_vp"] != {}:
            with open(AUGMENTED_PLANS_SAVE_DIR / f"{augmented_plan['rebalance_plan_json_key']}.json", "w") as f:
                json.dump(augmented_plan, f, indent=4)

    filtered_events = events[events["rebalance_file_path"].notna()]
    events_without_plans = events[events["rebalance_file_path"].isna()]
    print(f"Autopool: {autopool.name}")
    print(f"Number of events without rebalance plans: {len(events_without_plans)}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_and_save_augmented_plans, row) for idx, row in filtered_events.iterrows()]

        for future in tqdm(as_completed(futures), total=len(futures), desc="Augmenting and saving plans"):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing plan: {e}")


def run_old_plans():
    # Delete all existing augmented plans directories
    autopools = [AUTO_ETH, BASE_ETH, BAL_ETH, AUTO_LRT, DINERO_ETH]
    for autopool in autopools:
        augmented_plans_dir = WORKING_DATA_DIR / f"{autopool.name}_augmented_plans"
        if augmented_plans_dir.exists():
            shutil.rmtree(augmented_plans_dir)
            print(f"Deleted existing augmented plans directory: {augmented_plans_dir}")
    fetch_and_augment_onchain_calc_plans(AUTO_ETH)
    fetch_and_augment_onchain_calc_plans(BASE_ETH)
    # fetch_and_augment_onchain_calc_plans(BAL_ETH)
    # fetch_and_augment_onchain_calc_plans(AUTO_LRT)
    # fetch_and_augment_onchain_calc_plans(DINERO_ETH)


if __name__ == "__main__":
    run_old_plans()
