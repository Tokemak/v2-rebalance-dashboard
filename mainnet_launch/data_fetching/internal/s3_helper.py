"""

Central location where all s3 calls are made

Right now there is a s3 bucket that holds the autopool rebalance plan

Right now autoETH has 2 rebalance plan buckets 

"AUTO_ETH": environ["AUTO_ETH_BUCKET"],
"AUTO_ETH2": environ["AUTO_ETH_BUCKET2"],

AUTO_ETH Before Jan 3, 2026
AUTO_ETH2 After Jan 2, 2026

There is one rebalance plan that overlaps the dates

"""

from concurrent.futures import ThreadPoolExecutor
import json
import time

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from mainnet_launch.constants import AutopoolConstants, WORKING_DATA_DIR, S3_BUCKETS, AUTO_ETH, ALL_AUTOPOOLS, AUTO_USD

from mainnet_launch.database.postgres_operations import get_subset_of_table_as_df

from mainnet_launch.database.schema.full import RebalancePlans, RebalanceEvents


LOCAL_REBALANCE_ROOT = WORKING_DATA_DIR / "local_rebalance_plans"

# only autoETH has 2 buckets (as of Jan 9, 2026)
AUTOPOOL_TO_S3_BUCKETS = {a: (a.solver_rebalance_plans_bucket,) for a in ALL_AUTOPOOLS if a not in (AUTO_ETH, AUTO_USD)}
AUTOPOOL_TO_S3_BUCKETS[AUTO_ETH] = (S3_BUCKETS["AUTO_ETH2"], S3_BUCKETS["AUTO_ETH"])
AUTOPOOL_TO_S3_BUCKETS[AUTO_USD] = (S3_BUCKETS["AUTO_USD"], S3_BUCKETS["AUTO_USD2"])


def make_s3_client() -> boto3.client:
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def fetch_all_solver_rebalance_plan_file_names(autopool: AutopoolConstants, s3_client: boto3.client) -> list[str]:
    """Required for when the solver bucket has more than 1k objects"""
    keys = []
    continuation_token = None
    for bucket in AUTOPOOL_TO_S3_BUCKETS[autopool]:
        while True:
            kwargs = {"Bucket": bucket}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            resp = s3_client.list_objects_v2(**kwargs)

            for obj in resp.get("Contents", []):
                keys.append(obj["Key"])

            # Check if more results exist
            if resp.get("IsTruncated"):  # True means there’s another page
                continuation_token = resp["NextContinuationToken"]
            else:
                break

    return keys


def fetch_rebalance_plan_json_from_s3_bucket(plan_path: str, s3_client, autopool: AutopoolConstants):
    last = None
    for b in AUTOPOOL_TO_S3_BUCKETS[autopool]:
        try:
            plan = json.loads(s3_client.get_object(Bucket=b, Key=plan_path)["Body"].read())
            plan["rebalance_plan_json_key"] = plan_path
            plan["autopool_vault_address"] = autopool.autopool_eth_addr
            return plan
        except Exception as e:
            last = e
    raise RuntimeError(f"failed to download {plan_path} for autopool {autopool.name}") from last


def download_local_rebalance_plans():
    """
    iterates through each autopool, lists its s3 bucket of rebalance-plan jsons,
    creates a local subfolder, and downloads any missing json files in parallel.

    not used in production, but for adhoc analysis.
    """
    LOCAL_REBALANCE_ROOT.mkdir(parents=True, exist_ok=True)
    s3_client = make_s3_client()

    for autopool in ALL_AUTOPOOLS:
        for bucket_name in AUTOPOOL_TO_S3_BUCKETS[autopool]:
            subfolder = LOCAL_REBALANCE_ROOT / autopool.name
            subfolder.mkdir(parents=True, exist_ok=True)

            response = s3_client.list_objects_v2(Bucket=bucket_name)
            contents = response.get("Contents") or []
            all_keys = [obj["Key"] for obj in contents]

            existing_files = {p.name for p in subfolder.glob("*.json")}
            keys_to_fetch = [key for key in all_keys if key not in existing_files]

            if not keys_to_fetch:
                continue

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

            with ThreadPoolExecutor(max_workers=50) as executor:
                executor.map(download, keys_to_fetch)


def read_local_rebalance_plans(
    autopool: AutopoolConstants,
) -> list[dict]:
    subfolder = LOCAL_REBALANCE_ROOT / autopool.name
    plans = []
    for path in sorted(subfolder.glob("*.json")):
        with path.open("r", encoding="utf-8") as f:
            plans.append(json.load(f))

    return plans


if __name__ == "__main__":
    download_local_rebalance_plans()
