
import json
import pandas as pd

from mainnet_launch.app.app_config import  NUM_S3_BUCKET_FETCHING_THREADS

import time
import concurrent.futures
import boto3
from botocore import UNSIGNED
from botocore.config import Config

from mainnet_launch.constants import (
    AutopoolConstants,
    ALL_AUTOPOOLS,
    SOLVER_AUGMENTED_REBALANCE_PLANS_DIR,
    AUTO_USD, AUTO_ETH, ETH_CHAIN
)

from mainnet_launch.data_fetching.fetch_block_utils import get_nearest_block_before_timestamp_sync
from mainnet_launch.data_fetching.get_state_by_block import get_state_by_one_block, safe_normalize_with_bool_success
from mainnet_launch.pages.asset_discounts.fetch_usd_asset_discounts import stablecoin_tuples, _build_autoUSD_token_backing_calls
# I'm thinking about hard coding the LST backing calls


from multicall import Call


def _build_total_supply_call(name:str, token:str) -> Call:
    # works for curve and not for balancer
    return Call(
       name,
        ["totalSupply()(uint256)"],
        [(token, safe_normalize_with_bool_success)],
    )

def _build_getActualSupply_call(name:str, token:str) -> Call:
    return Call(
       name,
        ["getActualSupply()(uint256)"],
        [(token, safe_normalize_with_bool_success)],
    )


def augment_a_single_plan(autopool:AutopoolConstants = AUTO_USD, plan_name_on_remote:str = 'rebalance_plan_1743377226_0xa7569A44f348d3D70d8ad5889e50F78E33d80D35.json'):
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    rebalance_plan = json.loads(s3_client.get_object(Bucket=autopool.solver_rebalance_plans_bucket, Key=plan_name_on_remote)["Body"].read().decode())
    
    # only care about backing on ethereum by symbol
    timestamp, block = get_nearest_block_before_timestamp_sync(rebalance_plan['timestamp'], autopool.chain)
    rebalance_plan['block'] = block
    rebalance_plan['block_timestamp'] = timestamp

    backing_calls = _build_autoUSD_token_backing_calls()
    backing_dict = get_state_by_one_block(backing_calls, block, ETH_CHAIN)
    address_to_symbol = {a[0]:a[1] for a in stablecoin_tuples}
    symbol_to_address = {a[1]:a[0] for a in stablecoin_tuples}

    backing_dict = {symbol_to_address[k.replace('_backing', '')]:v for k, v in backing_dict.items() }
    
    total_supply_calls = 

    
    dest_states = rebalance_plan['sod']['destStates']
    
    for dest in dest_states:
        tokenBacking = [backing_dict[token] for token in dest['underlyingTokens']]
        dest['tokenBacking'] = tokenBacking
    
    pass




def fetch_asset_backing(rebalance_plan:dict, autopool:AutopoolConstants):
    # build
    pass



if __name__ == '__main__':
    augment_a_single_plan()
#     s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
#     response = s3_client.list_objects_v2(Bucket=AUTO_USD.solver_rebalance_plans_bucket)
#     solver_plans_names_on_remote = response.get("Contents")
#     all_rebalance_plans = [o["Key"] for o in solver_plans_names_on_remote]
#     print(all_rebalance_plans[0])
#     # pass
# pass




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
