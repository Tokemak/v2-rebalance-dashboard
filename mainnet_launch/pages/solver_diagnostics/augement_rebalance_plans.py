import json
import pandas as pd

from mainnet_launch.app.app_config import NUM_S3_BUCKET_FETCHING_THREADS

from multicall import Call

import time
import concurrent.futures
import boto3
from botocore import UNSIGNED
from botocore.config import Config

from mainnet_launch.constants import (
    AutopoolConstants,
    ALL_AUTOPOOLS,
    SOLVER_AUGMENTED_REBALANCE_PLANS_DIR,
    SOLVER_REBALANCE_PLANS_DIR,
    AUTO_USD,
    time_decorator,
)

from mainnet_launch.data_fetching.fetch_block_utils import get_nearest_block_before_timestamp_sync
from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    safe_normalize_with_bool_success,
)
from mainnet_launch.pages.asset_discounts.fetch_usd_asset_discounts import (
    stablecoin_tuples,
    _build_autoUSD_token_backing_calls,
)

from mainnet_launch.pages.asset_discounts.fetch_usd_asset_discounts import (
    stablecoin_tuples,
    _build_autoUSD_token_backing_calls,
)

from mainnet_launch.pages.asset_discounts.fetch_eth_asset_discounts import (
)



def _build_underlyingTotalSupply_call(name, vault: str) -> Call:
    return Call(
        vault,
        ["underlyingTotalSupply()(uint256)"],
        [(name, safe_normalize_with_bool_success)],
    )


def augment_a_single_plan(
    autopool: AutopoolConstants = AUTO_USD,
    plan_name_on_remote: str = "rebalance_plan_1743377226_0xa7569A44f348d3D70d8ad5889e50F78E33d80D35.json",
):
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    s3_client.download_file(
        autopool.solver_rebalance_plans_bucket,
        plan_name_on_remote,
        str(SOLVER_REBALANCE_PLANS_DIR / plan_name_on_remote),
    )
    with open(SOLVER_REBALANCE_PLANS_DIR / plan_name_on_remote, "r") as fin:
        rebalance_plan = json.load(fin)

    _add_extra_data_to_dest_states_in_stable_coin_rebalance_plan(autopool, rebalance_plan)

    with open(SOLVER_AUGMENTED_REBALANCE_PLANS_DIR / plan_name_on_remote, "w") as fout:
        json.dump(rebalance_plan, fout, indent=4)




def _add_extra_data_to_dest_states_in_stable_coin_rebalance_plan(autopool: AutopoolConstants, rebalance_plan: dict):

    block_timestamp, block = get_nearest_block_before_timestamp_sync(rebalance_plan["timestamp"], autopool.chain) # slowest part of setup
    rebalance_plan["block"] = block
    rebalance_plan["block_timestamp"] = block_timestamp
    dest_states = rebalance_plan["sod"]["destStates"]
    address_to_symbol = {a[0]: a[1] for a in stablecoin_tuples}
    backing_calls = _build_autoUSD_token_backing_calls()
    underlying_total_supply_calls = [
        _build_underlyingTotalSupply_call(dest["underlying"] + "_underlyingTotalSupply", dest["address"])
        for dest in dest_states
    ]

    onchain_data = get_state_by_one_block(
        [*backing_calls, *underlying_total_supply_calls],
        rebalance_plan["block"],
        autopool.chain,
    )

    for dest in dest_states:
        underlyingTokenSymbols = [address_to_symbol[token] for token in dest["underlyingTokens"]]
        dest["underlyingTokenSymbols"] = underlyingTokenSymbols
        dest["tokenBacking"] = [onchain_data[token + "_backing"] for token in underlyingTokenSymbols]
        dest["underlyingTotalSupply"] = onchain_data[dest["underlying"] + "_underlyingTotalSupply"]


if __name__ == "__main__":

    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    response = s3_client.list_objects_v2(Bucket=AUTO_USD.solver_rebalance_plans_bucket)
    solver_plans_names_on_remote = response.get("Contents")
    all_rebalance_plans = [o["Key"] for o in solver_plans_names_on_remote]

    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        # Schedule the function calls and map each future to its corresponding plan.
        futures = {
            executor.submit(augment_a_single_plan, AUTO_USD, plan): plan 
            for plan in all_rebalance_plans
        }
        
        # As each future completes, retrieve the result or handle exceptions.
        for future in concurrent.futures.as_completed(futures):
            plan = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                print(f"Plan {plan} generated an exception: {exc}")
            else:
                print(f"Plan {plan} processed successfully: {result}")


# old version
def ensure_all_rebalance_plans_are_loaded_from_s3_bucket():
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    for autopool in [ALL_AUTOPOOLS]:
        response = s3_client.list_objects_v2(Bucket=autopool.solver_rebalance_plans_bucket)
        solver_plans_names_on_remote = response.get("Contents")
        if solver_plans_names_on_remote is not None:
            all_rebalance_plans = [o["Key"] for o in solver_plans_names_on_remote]
            local_rebalance_plans = [str(path).split("/")[-1] for path in SOLVER_REBALANCE_PLANS_DIR.glob("*.json")]
            rebalance_plans_to_fetch = [
                json_path for json_path in all_rebalance_plans if json_path not in local_rebalance_plans
            ]
            if len(rebalance_plans_to_fetch) > 0:

                def download_file(json_key):
                    max_attempts = 3
                    for attempt in range(max_attempts):
                        try:
                            s3_client.download_file(
                                autopool.solver_rebalance_plans_bucket,
                                json_key,
                                str(SOLVER_REBALANCE_PLANS_DIR / json_key),
                            )
                            return
                        except Exception as e:
                            if attempt == max_attempts - 1:
                                # give up and fetch it sequentally later
                                return
                            else:
                                time.sleep((2**attempt) / 2)  # exponential backoff

                with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_S3_BUCKET_FETCHING_THREADS) as executor:
                    executor.map(download_file, rebalance_plans_to_fetch)

            # fetch any remaining that were not fetched seqentially
            updated_local_rebalance_plans = [
                str(path).split("/")[-1] for path in SOLVER_REBALANCE_PLANS_DIR.glob("*.json")
            ]
            leftover_rebalance_plans_to_fetch = [
                json_path for json_path in all_rebalance_plans if json_path not in updated_local_rebalance_plans
            ]
            for json_key in leftover_rebalance_plans_to_fetch:
                s3_client.download_file(
                    autopool.solver_rebalance_plans_bucket, json_key, SOLVER_REBALANCE_PLANS_DIR / json_key
                )


# def load_solver_plans(autopool: AutopoolConstants) -> list[dict]:
#     autopool_plans = [p for p in SOLVER_REBALANCE_PLANS_DIR.glob("*.json") if autopool.autopool_addr in str(p)]
#     plan_data = []
#     for plan_json in autopool_plans:
#         with open(plan_json, "r") as fin:
#             data = json.load(fin)
#             plan_data.append(data)

#     return plan_data


# # can be slow, requires loading a few thousand jsons.
# @st.cache_data(ttl=STREAMLIT_IN_MEMORY_CACHE_TIME)
# def _load_solver_df(autopool: AutopoolConstants) -> pd.DataFrame:
#     # not setup for if there are no rebalance plans
#     autopool_plans = [p for p in SOLVER_REBALANCE_PLANS_DIR.glob("*.json") if autopool.autopool_addr in str(p)]

#     if len(autopool_plans) == 0:
#         return None

#     destination_details = get_destination_details(autopool)
#     destination_vault_address_to_symbol = {dest.vaultAddress: dest.vault_name for dest in destination_details}
#     all_data = []
#     for plan_json in autopool_plans:
#         with open(plan_json, "r") as fin:
#             data = json.load(fin)
#             data["date"] = pd.to_datetime(data["timestamp"], unit="s", utc=True)
#             if data["destinationIn"] in destination_vault_address_to_symbol:
#                 data["destinationIn"] = destination_vault_address_to_symbol[data["destinationIn"]]

#             if data["destinationOut"] in destination_vault_address_to_symbol:
#                 data["destinationOut"] = destination_vault_address_to_symbol[data["destinationOut"]]

#             data["moveName"] = f"{data['destinationOut']} -> {data['destinationIn']}"
#             all_data.append(data)
#     solver_df = pd.DataFrame.from_records(all_data)
#     solver_df.sort_values("date", ascending=True, inplace=True)
#     return solver_df
