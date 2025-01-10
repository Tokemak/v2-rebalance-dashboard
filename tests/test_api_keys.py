"""Makes sure the API keys are correct"""

import boto3
from botocore.config import Config
from mainnet_launch.constants import ALL_AUTOPOOLS
from botocore import UNSIGNED
from mainnet_launch.constants import eth_client, base_client


def test_block_timestamp():
    assert eth_client.eth.get_block(20_000_000).timestamp == 1717281407
    assert base_client.eth.get_block(20_000_000).timestamp == 1726789347


def test_s3_bucket_access():
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    for autopool in ALL_AUTOPOOLS:
        try:
            bucket_name = autopool.solver_rebalance_plans_bucket
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            assert response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
        except Exception as e:
            print(autopool.name, "failed")
            raise e
