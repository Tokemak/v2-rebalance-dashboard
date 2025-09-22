import time
from concurrent.futures import ThreadPoolExecutor
import json

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from mainnet_launch.constants import WORKING_DATA_DIR, ALL_AUTOPOOLS, AutopoolConstants
from tqdm import tqdm


# assumes AutopoolConstants is already imported from your constants module
# and that WORKING_DATA_DIR is defined as the parent "working_data" folder

LOCAL_REBALANCE_ROOT = WORKING_DATA_DIR / "local_rebalance_plans"


def download_local_rebalance_plans(autopools: list[AutopoolConstants], max_workers: int = 50):
    """
    iterates through each autopool, lists its s3 bucket of rebalance-plan jsons,
    creates a local subfolder, and downloads any missing json files in parallel.
    """
    # make sure the root folder exists
    LOCAL_REBALANCE_ROOT.mkdir(parents=True, exist_ok=True)

    # s3 client with unsigned config (public buckets)
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    for autopool in autopools:
        bucket_name = autopool.solver_rebalance_plans_bucket
        subfolder = LOCAL_REBALANCE_ROOT / autopool.name
        subfolder.mkdir(parents=True, exist_ok=True)

        # get list of keys (json filenames) in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        contents = response.get("Contents") or []
        all_keys = [obj["Key"] for obj in contents]

        # find which keys are already downloaded
        existing_files = {p.name for p in subfolder.glob("*.json")}
        keys_to_fetch = [key for key in all_keys if key not in existing_files]

        if not keys_to_fetch:
            continue  # nothing to download for this autopool

        def download(key: str):
            attempts = 3
            for attempt in range(attempts):
                try:
                    target_path = subfolder / key
                    s3_client.download_file(bucket_name, key, str(target_path))
                    return
                except Exception as e:
                    if attempt == attempts - 1:
                        return
                    time.sleep((2**attempt) / 2)

        # download missing files in parallel with progress bar
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(tqdm(executor.map(download, keys_to_fetch), total=len(keys_to_fetch), desc=f"Downloading {autopool.name}"))


def fetch_one_plan(    autopool:AutopoolConstants, plan_path:str) -> dict:
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    
    response = s3_client.get_object(
        Bucket=autopool.solver_rebalance_plans_bucket,
        Key=plan_path,
    )
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)

if __name__ == "__main__":
    download_local_rebalance_plans(ALL_AUTOPOOLS)
