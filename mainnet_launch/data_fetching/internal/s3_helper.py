import json

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from mainnet_launch.constants.constants import AutopoolConstants


def make_s3_client() -> boto3.client:
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def fetch_all_solver_rebalance_plan_file_names(autopool: AutopoolConstants, s3_client: boto3.client) -> list[str]:
    """Required for when the solver bucket has more than 1k objects"""
    keys = []
    continuation_token = None

    while True:
        kwargs = {"Bucket": autopool.solver_rebalance_plans_bucket}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = s3_client.list_objects_v2(**kwargs)

        # Append this page's keys
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])

        # Check if more results exist
        if resp.get("IsTruncated"):  # True means there’s another page
            continuation_token = resp["NextContinuationToken"]
        else:
            break

    return keys


def fetch_rebalance_plan_json_from_s3_bucket(
    rebalance_plan_json_key: str, s3_client: boto3.client, autopool: AutopoolConstants
):
    plan = json.loads(
        s3_client.get_object(
            Bucket=autopool.solver_rebalance_plans_bucket,
            Key=rebalance_plan_json_key,
        )["Body"].read()
    )

    plan["rebalance_plan_json_key"] = rebalance_plan_json_key
    plan["autopool_vault_address"] = autopool.autopool_eth_addr
    return plan
